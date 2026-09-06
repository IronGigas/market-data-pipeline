// Команда ingestor читает поток сделок с биржи и публикует их в Kafka.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/IronGigas/market-data-pipeline/internal/broker/kafka"
	"github.com/IronGigas/market-data-pipeline/internal/config"
	"github.com/IronGigas/market-data-pipeline/internal/domain"
	"github.com/IronGigas/market-data-pipeline/internal/feed"
	"github.com/IronGigas/market-data-pipeline/internal/feed/binance"
)

// tradeBufferSize — ёмкость буфера между чтением из сокета и публикацией
// в Kafka.
//
// Буфер развязывает две скорости: биржа отдаёт сделки пачками, брокер
// принимает их со своей задержкой. При переполнении сделка отбрасывается,
// а чтение из сокета не останавливается ни на миг — отставание от рынка
// хуже потери отдельной сделки. Десять тысяч записей это несколько секунд
// потока по BTC-USDT: столько времени есть у брокера, чтобы прийти в себя.
const tradeBufferSize = 10000

func main() {
	// Конфиг читается до создания логгера: уровень логирования сам приходит
	// из конфига, а сообщать о его поломке нужно в любом случае.
	cfg, err := config.LoadIngestor()
	if err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("invalid config", slog.String("error", err.Error()))
		os.Exit(1)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))

	if err := run(cfg, log); err != nil {
		log.Error("ingestor failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}

func run(cfg config.Ingestor, log *slog.Logger) error {
	// NotifyContext закрывает контекст по Ctrl+C и по SIGTERM от docker stop.
	// Второй сигнал не перехватывается и убивает процесс принудительно —
	// штатный выход при этом не гарантируется, и это правильно: пользователь
	// уже попросил остановиться немедленно.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	producer, err := kafka.NewProducer(ctx, kafka.ProducerConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.TopicTrades,
		Logger:  log,
	})
	if err != nil {
		return err
	}

	client, err := binance.New(binance.Config{
		URL:     cfg.BinanceWSURL,
		Symbols: cfg.Symbols,
		Logger:  log,
	})
	if err != nil {
		// Продюсер уже подключён — закрываем, иначе останется висеть
		// соединение с брокером.
		_ = producer.Close()
		return err
	}

	// Переменная объявлена типом интерфейса, чтобы сервис зависел от feed.Feed,
	// а не от конкретной биржи.
	var source feed.Feed = client

	log.Info("ingestor started",
		slog.Any("symbols", cfg.Symbols),
		slog.Any("brokers", cfg.KafkaBrokers),
		slog.String("topic", cfg.TopicTrades),
		slog.String("log_level", cfg.LogLevel.String()))

	trades := make(chan domain.Trade, tradeBufferSize)
	publisherDone := make(chan struct{})

	// Публикация вынесена в отдельную горутину: даже кратковременная задержка
	// брокера не должна доходить до чтения из сокета.
	go func() {
		defer close(publisherDone)
		for trade := range trades {
			// Контекст сервиса здесь не годится: он отменяется первым, а
			// хвост буфера нужно успеть отправить. Ограничение времени
			// берёт на себя Flush внутри Close.
			producer.PublishTrade(context.WithoutCancel(ctx), trade)
		}
	}()

	var dropped atomic.Int64
	runErr := source.Run(ctx, func(trade domain.Trade) error {
		select {
		case trades <- trade:
		default:
			// Неблокирующая отправка: очередь полна, сделку теряем осознанно.
			dropped.Add(1)
			log.Warn("trade dropped, buffer is full",
				slog.String("symbol", trade.Symbol.String()),
				slog.Int64("trade_id", trade.TradeID),
				slog.Int("buffer", tradeBufferSize))
		}
		return nil
	})

	// Порядок остановки важен: сначала прекращается приток сделок, затем
	// опустошается буфер, и только потом сливается буфер самого продюсера.
	close(trades)
	<-publisherDone
	closeErr := producer.Close()

	feedStats := client.Stats()
	producerStats := producer.Stats()
	log.Info("ingestor stopped",
		slog.Int64("received", feedStats.Received),
		slog.Int64("published", producerStats.Published),
		slog.Int64("publish_failed", producerStats.Failed),
		slog.Int64("dropped", dropped.Load()),
		slog.Int64("duplicates", feedStats.Duplicates),
		slog.Int64("skipped", feedStats.Skipped),
		slog.Int64("malformed", feedStats.Failed),
		slog.Int64("reconnects", feedStats.Reconnects))

	if runErr != nil {
		return runErr
	}
	return closeErr
}
