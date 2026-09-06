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

// defaultFlushTimeout ограничивает ожидание отправки буфера при остановке.
// Пять секунд — компромисс: достаточно, чтобы слить накопленное в живой
// брокер, и мало, чтобы не подвешивать остановку при мёртвом.
const defaultFlushTimeout = 5 * time.Second

// ProducerConfig — параметры продюсера.
type ProducerConfig struct {
	Brokers []string
	Topic   string
	Logger  *slog.Logger

	// ClientID виден в метриках и логах брокера. Полезен, когда в кластер
	// пишут оба сервиса: по нему сразу понятно, чей это клиент.
	ClientID string

	FlushTimeout time.Duration
}

// ProducerStats — счётчики продюсера на момент вызова.
type ProducerStats struct {
	Published int64 // подтверждённые брокером записи
	Failed    int64 // записи, которые брокер не принял
}

// Producer публикует записи в один топик: сделки в md.trades или свечи
// в md.candles, в зависимости от того, кто его создал.
//
// Публикация асинхронная: Produce ставит запись в буфер клиента и сразу
// возвращает управление, а результат приходит в колбэк. Синхронная отправка
// с ожиданием подтверждения на каждую сделку упёрлась бы в сетевую задержку
// и не успевала бы за потоком.
type Producer struct {
	client *kgo.Client
	topic  string
	log    *slog.Logger

	flushTimeout time.Duration

	published atomic.Int64
	failed    atomic.Int64
}

// NewProducer подключается к брокерам и проверяет связь.
func NewProducer(ctx context.Context, cfg ProducerConfig) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: no brokers configured")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, errors.New("kafka: empty topic")
	}
	if cfg.Logger == nil {
		return nil, errors.New("kafka: logger is required")
	}

	flushTimeout := cfg.FlushTimeout
	if flushTimeout <= 0 {
		flushTimeout = defaultFlushTimeout
	}

	clientID := strings.TrimSpace(cfg.ClientID)
	if clientID == "" {
		clientID = "mdp-producer"
	}

	// Идемпотентный продюсер включён в franz-go по умолчанию: он снимает
	// дубликаты, которые иначе появились бы при внутренних повторах
	// отправки. Явно не отключаем.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.ClientID(clientID),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: create client: %w", err)
	}

	// Ping на старте: без него первая же ошибка адреса брокера всплыла бы
	// только в колбэке первой сделки, через минуту после запуска.
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("kafka: ping brokers %v: %w", cfg.Brokers, err)
	}

	return &Producer{
		client:       client,
		topic:        cfg.Topic,
		log:          cfg.Logger,
		flushTimeout: flushTimeout,
	}, nil
}

// PublishTrade ставит сделку в очередь отправки в топик сделок.
func (p *Producer) PublishTrade(ctx context.Context, trade domain.Trade) {
	value, err := EncodeTrade(trade)
	if err != nil {
		p.encodeFailed("trade", err,
			slog.String("symbol", trade.Symbol.String()),
			slog.Int64("trade_id", trade.TradeID))
		return
	}

	p.publish(ctx, TradeKey(trade), value, "trade",
		slog.String("symbol", trade.Symbol.String()),
		slog.Int64("trade_id", trade.TradeID))
}

// PublishCandle ставит закрытую свечу в очередь отправки в топик свечей.
//
// Публикуются только закрытые свечи: потребитель топика вправе считать
// каждое сообщение окончательным результатом по своему окну.
func (p *Producer) PublishCandle(ctx context.Context, candle domain.Candle) {
	value, err := EncodeCandle(candle)
	if err != nil {
		p.encodeFailed("candle", err,
			slog.String("symbol", candle.Symbol.String()),
			slog.String("timeframe", candle.Timeframe.String()))
		return
	}

	p.publish(ctx, CandleKey(candle), value, "candle",
		slog.String("symbol", candle.Symbol.String()),
		slog.String("timeframe", candle.Timeframe.String()),
		slog.Time("open_time", candle.OpenTime))
}

// publish отправляет запись асинхронно.
//
// Ошибки не возвращаются, а считаются и логируются: одна непринятая запись
// не повод останавливать конвейер, а вызывающий код всё равно не смог бы
// сделать с ней ничего осмысленного. Для свечей это безопасно ещё и потому,
// что они уже лежат в базе — топик здесь вторичен.
func (p *Producer) publish(ctx context.Context, key, value []byte, kind string, attrs ...slog.Attr) {
	record := &kgo.Record{
		Topic: p.topic,
		Key:   key,
		Value: value,
	}

	p.client.Produce(ctx, record, func(_ *kgo.Record, err error) {
		if err != nil {
			p.failed.Add(1)
			p.log.LogAttrs(context.Background(), slog.LevelError, "publish failed",
				append([]slog.Attr{
					slog.String("kind", kind),
					slog.String("error", err.Error()),
				}, attrs...)...)
			return
		}
		p.published.Add(1)
	})
}

func (p *Producer) encodeFailed(kind string, err error, attrs ...slog.Attr) {
	p.failed.Add(1)
	p.log.LogAttrs(context.Background(), slog.LevelError, "encode failed",
		append([]slog.Attr{
			slog.String("kind", kind),
			slog.String("error", err.Error()),
		}, attrs...)...)
}

// Stats возвращает снимок счётчиков. Безопасен для вызова из другой горутины.
func (p *Producer) Stats() ProducerStats {
	return ProducerStats{
		Published: p.published.Load(),
		Failed:    p.failed.Load(),
	}
}

// Close дожидается отправки буфера и закрывает клиента.
//
// Собственный контекст с таймаутом, а не контекст сервиса: на момент
// остановки контекст сервиса уже отменён, и Flush с ним вернулся бы
// немедленно, потеряв всё накопленное.
func (p *Producer) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), p.flushTimeout)
	defer cancel()

	err := p.client.Flush(ctx)
	p.client.Close()

	if err != nil {
		return fmt.Errorf("kafka: flush producer: %w", err)
	}
	return nil
}
