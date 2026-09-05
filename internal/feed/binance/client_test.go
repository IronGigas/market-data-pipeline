package binance

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

func testClient(t *testing.T) *Client {
	t.Helper()

	c, err := New(Config{
		URL:     "wss://stream.binance.com:9443/stream",
		Symbols: []domain.Symbol{"BTC-USDT", "ETH-USDT", "BTC-USDC"},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	return c
}

func TestNewValidatesConfig(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	tests := []struct {
		name string
		cfg  Config
	}{
		{name: "пустой URL", cfg: Config{Symbols: []domain.Symbol{"BTC-USDT"}, Logger: logger}},
		{name: "без символов", cfg: Config{URL: "wss://example", Logger: logger}},
		{name: "без логгера", cfg: Config{URL: "wss://example", Symbols: []domain.Symbol{"BTC-USDT"}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := New(tc.cfg)
			require.Error(t, err)
		})
	}
}

func TestStreamURL(t *testing.T) {
	t.Parallel()

	c := testClient(t)

	got, err := c.streamURL()
	require.NoError(t, err)

	// Список стримов уходит в query-параметр; url.Values экранирует символ @.
	require.Equal(t,
		"wss://stream.binance.com:9443/stream?streams=btcusdt%40trade%2Fethusdt%40trade%2Fbtcusdc%40trade",
		got)
}

// TestIsDuplicate проверяет дедупликацию, которая нужна после реконнекта:
// биржа может прислать хвост уже виденных сделок.
func TestIsDuplicate(t *testing.T) {
	t.Parallel()

	c := testClient(t)

	trade := func(symbol domain.Symbol, id int64) domain.Trade {
		return domain.Trade{
			Symbol:    symbol,
			TradeID:   id,
			Price:     decimal.NewFromInt(1),
			Quantity:  decimal.NewFromInt(1),
			EventTime: time.UnixMilli(1757074530120).UTC(),
			Source:    SourceName,
		}
	}

	require.False(t, c.isDuplicate(trade("BTC-USDT", 10)), "первая сделка не дубликат")
	require.True(t, c.isDuplicate(trade("BTC-USDT", 10)), "тот же TradeID — дубликат")
	require.True(t, c.isDuplicate(trade("BTC-USDT", 9)), "меньший TradeID — дубликат")
	require.False(t, c.isDuplicate(trade("BTC-USDT", 11)), "больший TradeID — новая сделка")

	// Счётчик ведётся отдельно по каждому инструменту: TradeID сквозной
	// только внутри символа.
	require.False(t, c.isDuplicate(trade("ETH-USDT", 5)), "чужой инструмент не задет")
	require.True(t, c.isDuplicate(trade("BTC-USDT", 11)))
}

func TestBackoff(t *testing.T) {
	t.Parallel()

	c := testClient(t)

	// Ожидаемая база до джиттера: 1s, 2s, 4s, 8s, 16s, дальше потолок 30s.
	bases := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
		30 * time.Second,
		30 * time.Second,
	}

	for attempt, base := range bases {
		low := time.Duration(float64(base) * 0.8)
		high := time.Duration(float64(base) * 1.2)

		// Джиттер случайный, поэтому проверяются границы, а не значение,
		// и на нескольких прогонах — чтобы поймать выход за диапазон.
		for range 50 {
			got := c.backoff(attempt + 1)
			require.GreaterOrEqual(t, got, low, "attempt=%d", attempt+1)
			require.LessOrEqual(t, got, high, "attempt=%d", attempt+1)
		}
	}
}
