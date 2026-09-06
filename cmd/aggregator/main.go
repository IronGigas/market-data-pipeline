// Команда aggregator читает сделки из Kafka, собирает их в OHLCV-свечи и
// выводит закрытые свечи в stdout.
//
// Запись в PostgreSQL и публикация в md.candles появятся следующими этапами;
// сейчас сервис доказывает, что окна закрываются вовремя и на реальном
// потоке получаются осмысленные свечи.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IronGigas/market-data-pipeline/internal/aggregate"
	"github.com/IronGigas/market-data-pipeline/internal/broker/kafka"
	"github.com/IronGigas/market-data-pipeline/internal/config"
	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

// pollTimeout — сколько ждать новых записей за один опрос Kafka.
//
// Это же период проверки дедлайнов: при пустом топике опрос возвращается
// через этот интервал, и цикл всё равно доходит до проверки окон. Поэтому
// отдельная горутина-тикер не нужна — окно замолчавшего инструмента
// закроется не позже, чем через 250 мс после своего дедлайна.
const pollTimeout = 250 * time.Millisecond

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

	consumer, err := kafka.NewConsumer(ctx, kafka.ConsumerConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.TopicTrades,
		Group:   cfg.ConsumerGroup,
		Logger:  log,
	})
	if err != nil {
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
		slog.String("topic", cfg.TopicTrades),
		slog.String("group", cfg.ConsumerGroup),
		slog.Duration("grace", cfg.Grace),
		slog.Duration("idle_timeout", cfg.IdleTimeout),
		slog.String("log_level", cfg.LogLevel.String()))

	loopErr := consume(ctx, consumer, aggregator, log)

	// Остановка: открытые окна дописываются как частичные свечи, иначе
	// накопленные сделки последнего интервала пропали бы бесследно.
	if partial := aggregator.Flush(); len(partial) > 0 {
		log.Info("flushing open windows", slog.Int("candles", len(partial)))
		emitCandles(log, partial)
	}

	stats := aggregator.Stats()
	consumerStats := consumer.Stats()
	log.Info("aggregator stopped",
		slog.Int64("trades", stats.Trades),
		slog.Int64("late", stats.Late),
		slog.Int64("records", consumerStats.Records),
		slog.Int64("malformed", consumerStats.Failed),
		slog.Int64("commits", consumerStats.Commits),
		slog.Any("closed", closedByTimeframe(stats)))

	return loopErr
}

// consume крутит основной цикл до отмены контекста.
//
// Цикл однопоточный намеренно. Проверка дедлайнов идёт в том же потоке, что
// и обработка батча: опрос Kafka сам возвращается по таймауту, и отдельная
// горутина-тикер только добавила бы гонку между свечами, закрытыми тикером,
// и коммитом оффсетов батча.
func consume(ctx context.Context, consumer *kafka.Consumer, aggregator *aggregate.Aggregator, log *slog.Logger) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		if err := consumeOnce(ctx, consumer, aggregator, log); err != nil {
			if ctx.Err() != nil || errors.Is(err, kafka.ErrConsumerClosed) {
				return nil
			}
			return err
		}
	}
}

// consumeOnce обрабатывает один батч: читает, наполняет окна, закрывает
// созревшие, отдаёт свечи и коммитит оффсеты.
func consumeOnce(ctx context.Context, consumer *kafka.Consumer, aggregator *aggregate.Aggregator, log *slog.Logger) error {
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
	emitCandles(log, aggregator.Expired(time.Now()))

	// Пустой батч коммитить незачем — оффсеты не сдвинулись.
	if len(trades) == 0 {
		return nil
	}

	// Коммит последним: до этого момента аварийная остановка приведёт лишь
	// к повторной обработке батча, а она безопасна.
	if err := consumer.Commit(ctx); err != nil {
		return err
	}
	return nil
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
