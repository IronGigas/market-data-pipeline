// Команда aggregator читает сделки из Kafka, собирает их в OHLCV-свечи,
// сохраняет закрытые свечи в PostgreSQL и публикует их в топик md.candles.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IronGigas/market-data-pipeline/internal/aggregate"
	"github.com/IronGigas/market-data-pipeline/internal/broker/kafka"
	"github.com/IronGigas/market-data-pipeline/internal/config"
	"github.com/IronGigas/market-data-pipeline/internal/domain"
	"github.com/IronGigas/market-data-pipeline/internal/storage/postgres"
)

// pollTimeout — сколько ждать новых записей за один опрос Kafka.
//
// Это же период проверки дедлайнов: при пустом топике опрос возвращается
// через этот интервал, и цикл всё равно доходит до проверки окон. Поэтому
// отдельная горутина-тикер не нужна — окно замолчавшего инструмента
// закроется не позже, чем через 250 мс после своего дедлайна.
const pollTimeout = 250 * time.Millisecond

// Повторы записи в базу.
//
// Три попытки с растущей паузой покрывают короткий сбой сети или
// перезапуск базы. Если и они не помогли, процесс падает: продолжать
// потребление, теряя свечи, хуже, чем остановиться — оффсеты не
// закоммичены, и после перезапуска батч будет обработан заново.
const (
	dbWriteAttempts     = 3
	dbWriteInitialDelay = 200 * time.Millisecond
)

// flushTimeout ограничивает запись частичных свечей при остановке: висеть
// на недоступной базе, когда пользователь уже нажал Ctrl+C, нельзя.
const flushTimeout = 5 * time.Second

func main() {
	cfg, err := config.LoadAggregator()
	if err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("invalid config", slog.String("error", err.Error()))
		os.Exit(1)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))

	if err := run(cfg, log); err != nil {
		log.Error("aggregator failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(cfg config.Aggregator, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// База открывается первой: без неё сервис бессмыслен, и незачем
	// вступать в consumer-группу, чтобы тут же из неё выйти.
	db, err := postgres.New(ctx, postgres.Config{
		DSN:    cfg.PostgresDSN,
		Logger: log,
	})
	if err != nil {
		return err
	}
	defer db.Close()

	candles := postgres.NewCandleStore(db)

	producer, err := kafka.NewProducer(ctx, kafka.ProducerConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.TopicCandles,
		ClientID: "mdp-aggregator",
		Logger:   log,
	})
	if err != nil {
		return err
	}

	consumer, err := kafka.NewConsumer(ctx, kafka.ConsumerConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.TopicTrades,
		Group:   cfg.ConsumerGroup,
		Logger:  log,
	})
	if err != nil {
		_ = producer.Close()
		return err
	}
	defer consumer.Close()

	aggregator, err := aggregate.New(aggregate.Config{
		Timeframes:  cfg.Timeframes,
		Grace:       cfg.Grace,
		IdleTimeout: cfg.IdleTimeout,
		Logger:      log,
	})
	if err != nil {
		return err
	}

	log.Info("aggregator started",
		slog.Any("timeframes", cfg.Timeframes),
		slog.Any("brokers", cfg.KafkaBrokers),
		slog.String("topic_trades", cfg.TopicTrades),
		slog.String("topic_candles", cfg.TopicCandles),
		slog.String("group", cfg.ConsumerGroup),
		slog.Duration("grace", cfg.Grace),
		slog.Duration("idle_timeout", cfg.IdleTimeout),
		slog.String("log_level", cfg.LogLevel.String()))

	sink := &candleSink{store: candles, producer: producer, log: log}

	loopErr := consume(ctx, consumer, sink, aggregator)

	// Остановка: открытые окна дописываются как частичные свечи, иначе
	// накопленные сделки последнего интервала пропали бы бесследно.
	if partial := aggregator.Flush(); len(partial) > 0 {
		log.Info("flushing open windows", slog.Int("candles", len(partial)))

		// Контекст сервиса уже отменён сигналом, поэтому для последней
		// записи берётся свежий с собственным таймаутом.
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), flushTimeout)
		if err := sink.write(flushCtx, partial); err != nil {
			log.Error("failed to persist partial candles", slog.String("error", err.Error()))
		}
		cancel()
	}

	// Продюсер закрывается после последней записи: Flush внутри Close
	// дожидается отправки накопленных свечей.
	closeErr := producer.Close()

	stats := aggregator.Stats()
	consumerStats := consumer.Stats()
	producerStats := producer.Stats()
	log.Info("aggregator stopped",
		slog.Int64("trades", stats.Trades),
		slog.Int64("late", stats.Late),
		slog.Int64("records", consumerStats.Records),
		slog.Int64("malformed", consumerStats.Failed),
		slog.Int64("commits", consumerStats.Commits),
		slog.Int64("candles_published", producerStats.Published),
		slog.Int64("publish_failed", producerStats.Failed),
		slog.Any("closed", closedByTimeframe(stats)))

	if loopErr != nil {
		return loopErr
	}
	return closeErr
}

// consume крутит основной цикл до отмены контекста.
//
// Цикл однопоточный намеренно. Проверка дедлайнов идёт в том же потоке, что
// и обработка батча: опрос Kafka сам возвращается по таймауту, и отдельная
// горутина-тикер только добавила бы гонку между свечами, закрытыми тикером,
// и коммитом оффсетов батча.
func consume(ctx context.Context, consumer *kafka.Consumer, sink *candleSink, aggregator *aggregate.Aggregator) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		if err := consumeOnce(ctx, consumer, sink, aggregator); err != nil {
			if ctx.Err() != nil || errors.Is(err, kafka.ErrConsumerClosed) {
				return nil
			}
			return err
		}
	}
}

// consumeOnce обрабатывает один батч: читает, наполняет окна, закрывает
// созревшие, отдаёт свечи и коммитит оффсеты.
func consumeOnce(ctx context.Context, consumer *kafka.Consumer, sink *candleSink, aggregator *aggregate.Aggregator) error {
	// Release снимает блокировку ребалансировки, поставленную Poll, при
	// любом исходе обработки батча.
	defer consumer.Release()

	trades, err := consumer.Poll(ctx, pollTimeout)
	if err != nil {
		return err
	}

	for _, trade := range trades {
		aggregator.Add(trade)
	}

	// Проверка дедлайнов идёт всегда, даже если батч пустой: по редкому
	// инструменту окно должно закрыться и без новых сделок.
	if err := sink.write(ctx, aggregator.Expired(time.Now())); err != nil {
		return err
	}

	// Пустой батч коммитить незачем — оффсеты не сдвинулись.
	if len(trades) == 0 {
		return nil
	}

	// Коммит последним, уже после успешной записи свечей: до этого момента
	// аварийная остановка приведёт лишь к повторной обработке батча,
	// а она безопасна благодаря идемпотентному upsert.
	if err := consumer.Commit(ctx); err != nil {
		return err
	}
	return nil
}

// candleSink принимает закрытые свечи: пишет их в базу, публикует в Kafka
// и выводит в лог.
//
// Порядок операций важен и зафиксирован здесь, а не размазан по вызывающему
// коду. База — источник истины, запись в неё блокирующая и с повторами.
// Топик вторичен: публикация асинхронная, её сбой не останавливает конвейер,
// потому что свеча уже сохранена и потребитель топика может перечитать её
// из базы.
type candleSink struct {
	store    *postgres.CandleStore
	producer *kafka.Producer
	log      *slog.Logger
}

func (s *candleSink) write(ctx context.Context, closed []domain.Candle) error {
	if len(closed) == 0 {
		return nil
	}

	if err := s.persist(ctx, closed); err != nil {
		return err
	}

	for _, candle := range closed {
		s.producer.PublishCandle(ctx, candle)
	}

	emitCandles(s.log, closed)
	return nil
}

// persist записывает свечи в базу, повторяя попытку при сбое.
//
// Оффсеты не коммитятся, пока запись не удалась, поэтому исчерпание попыток
// означает остановку сервиса: потерянные свечи будут пересчитаны при
// следующем запуске из того же батча.
func (s *candleSink) persist(ctx context.Context, closed []domain.Candle) error {
	delay := dbWriteInitialDelay
	var lastErr error

	for attempt := 1; attempt <= dbWriteAttempts; attempt++ {
		lastErr = s.store.Upsert(ctx, closed)
		if lastErr == nil {
			return nil
		}
		// Отмена контекста — это остановка сервиса, а не сбой базы.
		if ctx.Err() != nil {
			return lastErr
		}

		s.log.Error("failed to write candles",
			slog.Int("candles", len(closed)),
			slog.Int("attempt", attempt),
			slog.Int("attempts", dbWriteAttempts),
			slog.String("error", lastErr.Error()))

		if attempt == dbWriteAttempts {
			break
		}

		select {
		case <-ctx.Done():
			return lastErr
		case <-time.After(delay):
		}
		delay *= 2
	}

	return fmt.Errorf("write candles after %d attempts: %w", dbWriteAttempts, lastErr)
}

// emitCandles выводит закрытые свечи.
//
// Минутные свечи идут на уровне Info по одной строке: их несколько штук в
// минуту, и они читаются глазами. Секундных до трёх в секунду — они уходят
// на Debug, иначе консоль зальёт и минутные в ней потеряются.
func emitCandles(log *slog.Logger, candles []domain.Candle) {
	for _, candle := range candles {
		level := slog.LevelDebug
		if candle.Timeframe == domain.TF1m {
			level = slog.LevelInfo
		}

		log.Log(context.Background(), level, "candle closed",
			slog.String("symbol", candle.Symbol.String()),
			slog.String("tf", candle.Timeframe.String()),
			slog.Time("open_time", candle.OpenTime),
			slog.String("o", candle.Open.String()),
			slog.String("h", candle.High.String()),
			slog.String("l", candle.Low.String()),
			slog.String("c", candle.Close.String()),
			slog.String("v", candle.Volume.String()),
			slog.Int64("n", candle.TradeCount))
	}
}

// closedByTimeframe приводит счётчики к виду, пригодному для лога.
func closedByTimeframe(stats aggregate.Stats) map[string]int64 {
	closed := make(map[string]int64, len(stats.Closed))
	for tf, n := range stats.Closed {
		closed[tf.String()] = n
	}
	return closed
}
