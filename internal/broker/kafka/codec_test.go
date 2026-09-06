package kafka

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

func testTrade() domain.Trade {
	return domain.Trade{
		Symbol:    "BTC-USDT",
		TradeID:   4829371,
		Price:     decimal.RequireFromString("64250.15"),
		Quantity:  decimal.RequireFromString("0.00123"),
		EventTime: time.Date(2026, 9, 3, 10, 15, 30, 123_000_000, time.UTC),
		Source:    "binance",
	}
}

// TestEncodeTradeFormat фиксирует контракт топика md.trades: это сообщение
// читают глазами в Kafka UI и разбирает второй сервис, поэтому изменение
// формата должно ломать тест, а не потребителя.
func TestEncodeTradeFormat(t *testing.T) {
	t.Parallel()

	raw, err := EncodeTrade(testTrade())
	require.NoError(t, err)

	require.JSONEq(t, `{
		"symbol": "BTC-USDT",
		"trade_id": 4829371,
		"price": "64250.15",
		"quantity": "0.00123",
		"event_time": "2026-09-03T10:15:30.123Z",
		"source": "binance"
	}`, string(raw))
}

// TestEncodeTradeMoneyIsString проверяет главное решение формата: денежные
// величины уходят строками. Числовой литерал в JSON любой потребитель
// разберёт в float64 и потеряет последние знаки.
func TestEncodeTradeMoneyIsString(t *testing.T) {
	t.Parallel()

	raw, err := EncodeTrade(testTrade())
	require.NoError(t, err)

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))

	require.Equal(t, `"64250.15"`, string(fields["price"]))
	require.Equal(t, `"0.00123"`, string(fields["quantity"]))
}

// TestEncodeTradeTimeAlwaysUTC проверяет, что метка времени приводится к UTC
// независимо от зоны исходного времени: потребитель сравнивает моменты
// строками при чтении логов.
func TestEncodeTradeTimeAlwaysUTC(t *testing.T) {
	t.Parallel()

	trade := testTrade()
	trade.EventTime = trade.EventTime.In(time.FixedZone("MSK", 3*60*60))

	raw, err := EncodeTrade(trade)
	require.NoError(t, err)

	var msg tradeMessage
	require.NoError(t, json.Unmarshal(raw, &msg))
	require.Equal(t, "2026-09-03T10:15:30.123Z", msg.EventTime)
}

// TestEncodeTradeMillisecondsAlwaysThreeDigits ловит подмену формата на
// RFC3339Nano, который отбрасывает незначащие нули и делает ширину поля
// плавающей.
func TestEncodeTradeMillisecondsAlwaysThreeDigits(t *testing.T) {
	t.Parallel()

	trade := testTrade()
	trade.EventTime = time.Date(2026, 9, 3, 10, 15, 30, 120_000_000, time.UTC)

	raw, err := EncodeTrade(trade)
	require.NoError(t, err)

	var msg tradeMessage
	require.NoError(t, json.Unmarshal(raw, &msg))
	require.Equal(t, "2026-09-03T10:15:30.120Z", msg.EventTime)
}

func TestTradeRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		trade domain.Trade
	}{
		{name: "обычная сделка", trade: testTrade()},
		{
			name: "много знаков после запятой",
			trade: domain.Trade{
				Symbol:    "ETH-USDT",
				TradeID:   1,
				Price:     decimal.RequireFromString("123456789.123456789012345678"),
				Quantity:  decimal.RequireFromString("0.000000000000000001"),
				EventTime: time.Date(2026, 9, 3, 10, 15, 30, 1_000_000, time.UTC),
				Source:    "binance",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			raw, err := EncodeTrade(tc.trade)
			require.NoError(t, err)

			got, err := DecodeTrade(raw)
			require.NoError(t, err)

			require.Equal(t, tc.trade.Symbol, got.Symbol)
			require.Equal(t, tc.trade.TradeID, got.TradeID)
			require.Equal(t, tc.trade.Source, got.Source)
			require.True(t, tc.trade.Price.Equal(got.Price), "цена: %s != %s", tc.trade.Price, got.Price)
			require.True(t, tc.trade.Quantity.Equal(got.Quantity), "объём: %s != %s", tc.trade.Quantity, got.Quantity)
			require.True(t, tc.trade.EventTime.Equal(got.EventTime))
			require.Equal(t, time.UTC, got.EventTime.Location())
		})
	}
}

func TestDecodeTradeErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		raw    string
		wantIs error
	}{
		{name: "битый JSON", raw: `{"symbol":`},
		{
			name:   "символ не в доменной форме",
			raw:    `{"symbol":"BTCUSDT","trade_id":1,"price":"1","quantity":"1","event_time":"2026-09-03T10:15:30.123Z","source":"binance"}`,
			wantIs: domain.ErrInvalidSymbol,
		},
		{
			name: "цена не число",
			raw:  `{"symbol":"BTC-USDT","trade_id":1,"price":"abc","quantity":"1","event_time":"2026-09-03T10:15:30.123Z","source":"binance"}`,
		},
		{
			name: "время не по RFC3339",
			raw:  `{"symbol":"BTC-USDT","trade_id":1,"price":"1","quantity":"1","event_time":"03.09.2026 10:15","source":"binance"}`,
		},
		{
			name:   "нулевая цена не проходит доменную проверку",
			raw:    `{"symbol":"BTC-USDT","trade_id":1,"price":"0","quantity":"1","event_time":"2026-09-03T10:15:30.123Z","source":"binance"}`,
			wantIs: domain.ErrInvalidTrade,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := DecodeTrade([]byte(tc.raw))
			require.Error(t, err)
			if tc.wantIs != nil {
				require.ErrorIs(t, err, tc.wantIs)
			}
		})
	}
}

// TestDecodeTradeAcceptsOffsetTime проверяет, что потребитель примет метку
// со смещением и приведёт её к UTC: RFC3339 это допускает, а наши сравнения
// окон идут в UTC.
func TestDecodeTradeAcceptsOffsetTime(t *testing.T) {
	t.Parallel()

	raw := `{"symbol":"BTC-USDT","trade_id":1,"price":"1","quantity":"1","event_time":"2026-09-03T13:15:30.123+03:00","source":"binance"}`

	trade, err := DecodeTrade([]byte(raw))
	require.NoError(t, err)

	require.True(t, trade.EventTime.Equal(time.Date(2026, 9, 3, 10, 15, 30, 123_000_000, time.UTC)))
	require.Equal(t, time.UTC, trade.EventTime.Location())
}

func TestTradeKey(t *testing.T) {
	t.Parallel()

	// Ключ — ровно доменный символ: от этого зависит и партиционирование,
	// и читаемость вывода консольного консьюмера.
	require.Equal(t, []byte("BTC-USDT"), TradeKey(testTrade()))
}

func testCandle() domain.Candle {
	return domain.Candle{
		Symbol:     "BTC-USDT",
		Timeframe:  domain.TF1m,
		OpenTime:   time.Date(2026, 9, 3, 10, 15, 0, 0, time.UTC),
		CloseTime:  time.Date(2026, 9, 3, 10, 16, 0, 0, time.UTC),
		Open:       decimal.RequireFromString("64250.15"),
		High:       decimal.RequireFromString("64301"),
		Low:        decimal.RequireFromString("64244.8"),
		Close:      decimal.RequireFromString("64288.42"),
		Volume:     decimal.RequireFromString("12.4831"),
		TradeCount: 1842,
	}
}

// TestEncodeCandleFormat фиксирует контракт топика md.candles.
func TestEncodeCandleFormat(t *testing.T) {
	t.Parallel()

	raw, err := EncodeCandle(testCandle())
	require.NoError(t, err)

	require.JSONEq(t, `{
		"symbol": "BTC-USDT",
		"timeframe": "1m",
		"open_time": "2026-09-03T10:15:00.000Z",
		"close_time": "2026-09-03T10:16:00.000Z",
		"open": "64250.15",
		"high": "64301",
		"low": "64244.8",
		"close": "64288.42",
		"volume": "12.4831",
		"trade_count": 1842
	}`, string(raw))
}

func TestCandleRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		candle domain.Candle
	}{
		{name: "минутная свеча", candle: testCandle()},
		{
			name: "секундная свеча с длинным объёмом",
			candle: domain.Candle{
				Symbol:     "ETH-USDT",
				Timeframe:  domain.TF1s,
				OpenTime:   time.Date(2026, 9, 3, 10, 15, 30, 0, time.UTC),
				CloseTime:  time.Date(2026, 9, 3, 10, 15, 31, 0, time.UTC),
				Open:       decimal.RequireFromString("2453.87"),
				High:       decimal.RequireFromString("2453.87"),
				Low:        decimal.RequireFromString("2453.86"),
				Close:      decimal.RequireFromString("2453.86"),
				Volume:     decimal.RequireFromString("0.000000000000000001"),
				TradeCount: 1,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			raw, err := EncodeCandle(tc.candle)
			require.NoError(t, err)

			got, err := DecodeCandle(raw)
			require.NoError(t, err)

			require.Equal(t, tc.candle.Symbol, got.Symbol)
			require.Equal(t, tc.candle.Timeframe, got.Timeframe)
			require.True(t, tc.candle.OpenTime.Equal(got.OpenTime))
			require.True(t, tc.candle.CloseTime.Equal(got.CloseTime))
			require.Equal(t, tc.candle.TradeCount, got.TradeCount)

			for _, pair := range []struct {
				name     string
				want, is decimal.Decimal
			}{
				{"open", tc.candle.Open, got.Open},
				{"high", tc.candle.High, got.High},
				{"low", tc.candle.Low, got.Low},
				{"close", tc.candle.Close, got.Close},
				{"volume", tc.candle.Volume, got.Volume},
			} {
				require.True(t, pair.want.Equal(pair.is), "%s: %s != %s", pair.name, pair.want, pair.is)
			}
		})
	}
}

func TestDecodeCandleErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		raw    string
		wantIs error
	}{
		{name: "битый JSON", raw: `{"symbol":`},
		{
			name:   "неизвестный таймфрейм",
			raw:    `{"symbol":"BTC-USDT","timeframe":"1h","open_time":"2026-09-03T10:15:00.000Z","close_time":"2026-09-03T11:15:00.000Z","open":"1","high":"1","low":"1","close":"1","volume":"1","trade_count":1}`,
			wantIs: domain.ErrUnknownTimeframe,
		},
		{
			name:   "символ не в доменной форме",
			raw:    `{"symbol":"BTCUSDT","timeframe":"1m","open_time":"2026-09-03T10:15:00.000Z","close_time":"2026-09-03T10:16:00.000Z","open":"1","high":"1","low":"1","close":"1","volume":"1","trade_count":1}`,
			wantIs: domain.ErrInvalidSymbol,
		},
		{
			name: "объём не число",
			raw:  `{"symbol":"BTC-USDT","timeframe":"1m","open_time":"2026-09-03T10:15:00.000Z","close_time":"2026-09-03T10:16:00.000Z","open":"1","high":"1","low":"1","close":"1","volume":"x","trade_count":1}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := DecodeCandle([]byte(tc.raw))
			require.Error(t, err)
			if tc.wantIs != nil {
				require.ErrorIs(t, err, tc.wantIs)
			}
		})
	}
}

// TestCandleKey проверяет ключ компакции: без таймфрейма секундная свеча
// вытесняла бы минутную по тому же инструменту.
func TestCandleKey(t *testing.T) {
	t.Parallel()

	minute := testCandle()
	second := minute
	second.Timeframe = domain.TF1s

	require.Equal(t, []byte("BTC-USDT|1m"), CandleKey(minute))
	require.Equal(t, []byte("BTC-USDT|1s"), CandleKey(second))
	require.NotEqual(t, CandleKey(minute), CandleKey(second))
}
