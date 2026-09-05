package aggregate

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

// tradeAt собирает сделку с заданными ценой и объёмом.
func tradeAt(price, quantity string, eventTime time.Time) domain.Trade {
	return domain.Trade{
		Symbol:    "BTC-USDT",
		TradeID:   1,
		Price:     decimal.RequireFromString(price),
		Quantity:  decimal.RequireFromString(quantity),
		EventTime: eventTime,
		Source:    "binance",
	}
}

var testKey = WindowKey{Symbol: "BTC-USDT", Timeframe: domain.TF1m}

func TestNewWindowFromSingleTrade(t *testing.T) {
	t.Parallel()

	openTime := time.Date(2026, 9, 3, 10, 15, 0, 0, time.UTC)
	w := newWindow(testKey, openTime, tradeAt("64250.15", "0.5", openTime))

	// Свеча из одной сделки имеет нулевой размах, но не нулевые high и low.
	require.Equal(t, "64250.15", w.Open.String())
	require.Equal(t, "64250.15", w.High.String())
	require.Equal(t, "64250.15", w.Low.String())
	require.Equal(t, "64250.15", w.Close.String())
	require.Equal(t, "0.5", w.Volume.String())
	require.Equal(t, int64(1), w.TradeCount)
}

func TestWindowApply(t *testing.T) {
	t.Parallel()

	openTime := time.Date(2026, 9, 3, 10, 15, 0, 0, time.UTC)

	tests := []struct {
		name   string
		prices []string
		wantO  string
		wantH  string
		wantL  string
		wantC  string
	}{
		{
			name:   "цена растёт",
			prices: []string{"100", "110", "120"},
			wantO:  "100", wantH: "120", wantL: "100", wantC: "120",
		},
		{
			name:   "цена падает",
			prices: []string{"120", "110", "100"},
			wantO:  "120", wantH: "120", wantL: "100", wantC: "100",
		},
		{
			name:   "максимум и минимум в середине",
			prices: []string{"100", "130", "90", "105"},
			wantO:  "100", wantH: "130", wantL: "90", wantC: "105",
		},
		{
			name:   "цена не меняется",
			prices: []string{"100", "100", "100"},
			wantO:  "100", wantH: "100", wantL: "100", wantC: "100",
		},
		{
			name:   "максимум последней сделкой",
			prices: []string{"100", "90", "150"},
			wantO:  "100", wantH: "150", wantL: "90", wantC: "150",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			w := newWindow(testKey, openTime, tradeAt(tc.prices[0], "1", openTime))
			for _, price := range tc.prices[1:] {
				w.apply(tradeAt(price, "1", openTime))
			}

			require.Equal(t, tc.wantO, w.Open.String(), "open")
			require.Equal(t, tc.wantH, w.High.String(), "high")
			require.Equal(t, tc.wantL, w.Low.String(), "low")
			require.Equal(t, tc.wantC, w.Close.String(), "close")
			require.Equal(t, int64(len(tc.prices)), w.TradeCount)
		})
	}
}

// TestWindowVolumeIsExact проверяет, ради чего в проекте decimal: сумма
// объёмов на float64 накопила бы ошибку уже на трёх слагаемых.
func TestWindowVolumeIsExact(t *testing.T) {
	t.Parallel()

	openTime := time.Date(2026, 9, 3, 10, 15, 0, 0, time.UTC)

	w := newWindow(testKey, openTime, tradeAt("100", "0.1", openTime))
	w.apply(tradeAt("100", "0.2", openTime))
	w.apply(tradeAt("100", "0.3", openTime))

	require.Equal(t, "0.6", w.Volume.String())
}

func TestWindowCloseTime(t *testing.T) {
	t.Parallel()

	openTime := time.Date(2026, 9, 3, 10, 15, 0, 0, time.UTC)

	tests := []struct {
		name string
		tf   domain.Timeframe
		want time.Time
	}{
		{name: "1m", tf: domain.TF1m, want: time.Date(2026, 9, 3, 10, 16, 0, 0, time.UTC)},
		{name: "1s", tf: domain.TF1s, want: time.Date(2026, 9, 3, 10, 15, 1, 0, time.UTC)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			key := WindowKey{Symbol: "BTC-USDT", Timeframe: tc.tf}
			w := newWindow(key, openTime, tradeAt("100", "1", openTime))

			require.True(t, w.CloseTime().Equal(tc.want), "want %s, got %s", tc.want, w.CloseTime())
			require.True(t, w.deadline(2*time.Second).Equal(tc.want.Add(2*time.Second)))
		})
	}
}

func TestWindowCandle(t *testing.T) {
	t.Parallel()

	openTime := time.Date(2026, 9, 3, 10, 15, 0, 0, time.UTC)

	w := newWindow(testKey, openTime, tradeAt("100", "1.5", openTime))
	w.apply(tradeAt("130", "0.5", openTime))
	w.apply(tradeAt("90", "2", openTime))

	candle := w.Candle()

	require.Equal(t, domain.Symbol("BTC-USDT"), candle.Symbol)
	require.Equal(t, domain.TF1m, candle.Timeframe)
	require.True(t, candle.OpenTime.Equal(openTime))
	require.True(t, candle.CloseTime.Equal(openTime.Add(time.Minute)))
	require.Equal(t, "100", candle.Open.String())
	require.Equal(t, "130", candle.High.String())
	require.Equal(t, "90", candle.Low.String())
	require.Equal(t, "90", candle.Close.String())
	require.Equal(t, "4", candle.Volume.String())
	require.Equal(t, int64(3), candle.TradeCount)
}
