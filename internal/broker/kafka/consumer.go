package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

// ErrConsumerClosed возвращается Poll после закрытия клиента.
var ErrConsumerClosed = errors.New("kafka: consumer is closed")

// defaultMaxPollRecords ограничивает размер батча.
//
// Батч — единица работы между коммитами: чем он больше, тем дешевле коммит
// на запись, но тем больше сделок обработается повторно после аварийной
// остановки. Тысяча записей это несколько секунд потока по BTC-USDT.
const defaultMaxPollRecords = 1000

// ConsumerConfig — параметры консьюмера.
type ConsumerConfig struct {
	Brokers []string
	Topic   string
	Group   string
	Logger  *slog.Logger

	MaxPollRecords int
}

// ConsumerStats — счётчики консьюмера на момент вызова.
type ConsumerStats struct {
	Records int64 // разобранные записи, отданные наверх
	Failed  int64 // записи, которые не удалось разобрать
	Commits int64
}

// Consumer читает сделки из топика md.trades в составе consumer-группы.
//
// Автокоммит выключен: оффсет должен уезжать в брокер только после того,
// как свечи батча записаны. Семантика доставки — at-least-once: после
// аварийной остановки часть сделок обработается повторно, и это безопасно
// благодаря идемпотентному upsert по первичному ключу свечи.
type Consumer struct {
	client *kgo.Client
	topic  string
	group  string
	log    *slog.Logger

	maxPollRecords int

	records atomic.Int64
	failed  atomic.Int64
	commits atomic.Int64
}

// NewConsumer подключается к брокерам и вступает в группу.
func NewConsumer(ctx context.Context, cfg ConsumerConfig) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: no brokers configured")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, errors.New("kafka: empty topic")
	}
	if strings.TrimSpace(cfg.Group) == "" {
		return nil, errors.New("kafka: empty consumer group")
	}
	if cfg.Logger == nil {
		return nil, errors.New("kafka: logger is required")
	}

	maxPollRecords := cfg.MaxPollRecords
	if maxPollRecords <= 0 {
		maxPollRecords = defaultMaxPollRecords
	}

	c := &Consumer{
		topic:          cfg.Topic,
		group:          cfg.Group,
		log:            cfg.Logger,
		maxPollRecords: maxPollRecords,
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ClientID("mdp-aggregator"),

		// Коммит только вручную, после успешной обработки батча.
		kgo.DisableAutoCommit(),

		// Ребалансировка откладывается до конца обработки батча. Иначе
		// партицию могут отобрать между чтением и коммитом, и коммит
		// уйдёт по уже чужой партиции.
		kgo.BlockRebalanceOnPoll(),

		// Новая группа начинает с начала топика: свечи за уже накопленные
		// сделки полезнее, чем пустой график до первой новой сделки.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),

		kgo.OnPartitionsAssigned(c.onAssigned),
		kgo.OnPartitionsRevoked(c.onRevoked),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: create consumer: %w", err)
	}

	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("kafka: ping brokers %v: %w", cfg.Brokers, err)
	}

	c.client = client
	return c, nil
}

// Poll читает очередной батч и разбирает записи в доменные сделки.
//
// timeout ограничивает ожидание: при пустом топике Poll вернёт пустой батч,
// а не заблокируется. Это важно для вызывающего — между чтениями он проверяет
// дедлайны окон, и по редкому инструменту окно должно закрыться даже тогда,
// когда новых сделок нет вовсе.
//
// После Poll ребалансировка заблокирована до вызова Release.
func (c *Consumer) Poll(ctx context.Context, timeout time.Duration) ([]domain.Trade, error) {
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	fetches := c.client.PollRecords(pollCtx, c.maxPollRecords)
	if fetches.IsClientClosed() {
		return nil, ErrConsumerClosed
	}

	for _, fetchErr := range fetches.Errors() {
		// Истёкший таймаут опроса — это «данных пока нет», а не сбой.
		if errors.Is(fetchErr.Err, context.DeadlineExceeded) || errors.Is(fetchErr.Err, context.Canceled) {
			continue
		}
		// Остальные ошибки временные: franz-go сам переподключится и
		// перечитает партицию, поэтому батч не бракуется целиком.
		c.log.Warn("fetch error",
			slog.String("topic", fetchErr.Topic),
			slog.Int("partition", int(fetchErr.Partition)),
			slog.String("error", fetchErr.Err.Error()))
	}

	trades := make([]domain.Trade, 0, fetches.NumRecords())
	fetches.EachRecord(func(record *kgo.Record) {
		trade, err := DecodeTrade(record.Value)
		if err != nil {
			// Нечитаемая запись пропускается, а не останавливает конвейер:
			// иначе одно битое сообщение заблокировало бы партицию навсегда.
			c.failed.Add(1)
			c.log.Warn("skip malformed record",
				slog.Int("partition", int(record.Partition)),
				slog.Int64("offset", record.Offset),
				slog.String("error", err.Error()))
			return
		}
		trades = append(trades, trade)
	})

	c.records.Add(int64(len(trades)))
	return trades, nil
}

// Commit фиксирует оффсеты прочитанного батча.
func (c *Consumer) Commit(ctx context.Context) error {
	if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
		return fmt.Errorf("kafka: commit offsets: %w", err)
	}
	c.commits.Add(1)
	return nil
}

// Release снимает блокировку ребалансировки, поставленную Poll.
// Обязателен после каждого Poll, иначе группа не сможет перераспределить
// партиции при появлении второго экземпляра сервиса.
func (c *Consumer) Release() {
	c.client.AllowRebalance()
}

// Stats возвращает снимок счётчиков.
func (c *Consumer) Stats() ConsumerStats {
	return ConsumerStats{
		Records: c.records.Load(),
		Failed:  c.failed.Load(),
		Commits: c.commits.Load(),
	}
}

// Close покидает группу и закрывает соединения.
//
// Явный выход из группы ускоряет ребалансировку: без него оставшиеся
// участники ждали бы истечения session timeout.
func (c *Consumer) Close() {
	c.client.Close()
}

func (c *Consumer) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		c.log.Info("partitions assigned",
			slog.String("group", c.group),
			slog.String("topic", topic),
			slog.Any("partitions", partitions))
	}
}

func (c *Consumer) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	for topic, partitions := range revoked {
		c.log.Info("partitions revoked",
			slog.String("group", c.group),
			slog.String("topic", topic),
			slog.Any("partitions", partitions))
	}
}
