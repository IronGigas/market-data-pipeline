// Команда ingestor читает поток сделок с биржи и выводит их в stdout.
//
// Публикация в Kafka появится следующим этапом; сейчас сервис доказывает,
// что соединение с биржей живёт, сообщения разбираются и превращаются в
// доменные сделки.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/IronGigas/market-data-pipeline/internal/config"
	"github.com/IronGigas/market-data-pipeline/internal/domain"
	"github.com/IronGigas/market-data-pipeline/internal/feed"
	"github.com/IronGigas/market-data-pipeline/internal/feed/binance"
)

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

	client, err := binance.New(binance.Config{
		URL:     cfg.BinanceWSURL,
		Symbols: cfg.Symbols,
		Logger:  log,
	})
	if err != nil {
		return err
	}

	// Переменная объявлена типом интерфейса, чтобы сервис зависел от feed.Feed,
	// а не от конкретной биржи.
	var source feed.Feed = client

	log.Info("ingestor started",
		slog.Any("symbols", cfg.Symbols),
		slog.String("log_level", cfg.LogLevel.String()))

	err = source.Run(ctx, func(trade domain.Trade) error {
		printTrade(log, trade)
		return nil
	})
	if err != nil {
		return err
	}

	stats := client.Stats()
	log.Info("ingestor stopped",
		slog.Int64("received", stats.Received),
		slog.Int64("duplicates", stats.Duplicates),
		slog.Int64("skipped", stats.Skipped),
		slog.Int64("failed", stats.Failed),
		slog.Int64("reconnects", stats.Reconnects))

	return nil
}

// printTrade выводит сделку в лог. Цена и объём печатаются строками:
// форматирование через float потеряло бы точность ровно там, где она
// и нужна — в последних знаках.
func printTrade(log *slog.Logger, trade domain.Trade) {
	log.Info("trade",
		slog.String("symbol", trade.Symbol.String()),
		slog.Int64("trade_id", trade.TradeID),
		slog.String("price", trade.Price.String()),
		slog.String("quantity", trade.Quantity.String()),
		slog.Time("event_time", trade.EventTime))
}
