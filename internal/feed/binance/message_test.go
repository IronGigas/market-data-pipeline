package binance

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

// testSymbols — тот же список, что и в конфиге по умолчанию.
func testSymbols(t *testing.T) *symbolMap {
	t.Helper()

	m, err := newSymbolMap([]domain.Symbol{"BTC-USDT", "ETH-USDT", "BTC-USDC"})
	require.NoError(t, err)
	return m
}

// tradeMessageBTC — сообщение комбинированного потока в том виде, в каком его
// присылает Binance: обёртка stream/data, однобуквенные поля, цена и объём
// строками.
const tradeMessageBTC = `{"stream":"btcusdt@trade","data":{` +
	`"e":"trade","E":1757074530123,"s":"BTCUSDT","t":4829371,` +
	`"p":"64250.15000000","q":"0.00123000","T":1757074530120,"m":true,"M":true}}`

func TestParseTrade(t *testing.T) {
	t.Parallel()

	symbols := testSymbols(t)

	trade, err := parseTrade([]byte(tradeMessageBTC), symbols, SourceName)
	require.NoError(t, err)

	require.Equal(t, domain.Symbol("BTC-USDT"), trade.Symbol)
	require.Equal(t, int64(4829371), trade.TradeID)
	require.Equal(t, SourceName, trade.Source)
	require.True(t, decimal.RequireFromString("64250.15").Equal(trade.Price))
	require.True(t, decimal.RequireFromString("0.00123").Equal(trade.Quantity))

	// Время берётся из T (время сделки), а не из E (время отправки события):
	// поля различаются на 3 мс, и оконная агрегация обязана идти по первому.
	require.True(t, trade.EventTime.Equal(time.UnixMilli(1757074530120)))
	require.Equal(t, time.UTC, trade.EventTime.Location())
}

// TestParseTradeKeepsDecimalPrecision фиксирует главную причину, по которой
// цена и объём разбираются из строк в decimal: через float64 такие значения
// не проходят без потери последних знаков.
func TestParseTradeKeepsDecimalPrecision(t *testing.T) {
	t.Parallel()

	symbols := testSymbols(t)

	const (
		price    = "123456789.123456789012345678"
		quantity = "0.000000000000000001"
	)
	raw := `{"stream":"btcusdt@trade","data":{"e":"trade","s":"BTCUSDT","t":1,` +
		`"p":"` + price + `","q":"` + quantity + `","T":1757074530120}}`

	trade, err := parseTrade([]byte(raw), symbols, SourceName)
	require.NoError(t, err)

	require.Equal(t, price, trade.Price.String())
	require.Equal(t, quantity, trade.Quantity.String())
}

func TestParseTradeSymbolMapping(t *testing.T) {
	t.Parallel()

	symbols := testSymbols(t)

	tests := []struct {
		name     string
		ticker   string
		expected domain.Symbol
	}{
		{name: "BTCUSDT", ticker: "BTCUSDT", expected: "BTC-USDT"},
		{name: "ETHUSDT", ticker: "ETHUSDT", expected: "ETH-USDT"},
		// USDC отличается от USDT одной буквой в конце — на таких парах
		// и ломается попытка вычислить котировку по длине суффикса.
		{name: "BTCUSDC", ticker: "BTCUSDC", expected: "BTC-USDC"},
		{name: "нижний регистр", ticker: "btcusdt", expected: "BTC-USDT"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			raw := `{"stream":"x@trade","data":{"e":"trade","s":"` + tc.ticker +
				`","t":1,"p":"1","q":"1","T":1757074530120}}`

			trade, err := parseTrade([]byte(raw), symbols, SourceName)
			require.NoError(t, err)
			require.Equal(t, tc.expected, trade.Symbol)
		})
	}
}

func TestParseTradeSkipped(t *testing.T) {
	t.Parallel()

	symbols := testSymbols(t)

	tests := []struct {
		name string
		raw  string
	}{
		{
			name: "ответ на служебную команду без поля data",
			raw:  `{"result":null,"id":1}`,
		},
		{
			name: "событие другого типа",
			raw:  `{"stream":"btcusdt@kline_1m","data":{"e":"kline","s":"BTCUSDT"}}`,
		},
		{
			name: "инструмент, на который мы не подписаны",
			raw:  `{"stream":"solusdt@trade","data":{"e":"trade","s":"SOLUSDT","t":1,"p":"1","q":"1","T":1757074530120}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseTrade([]byte(tc.raw), symbols, SourceName)
			require.ErrorIs(t, err, errNotATrade)
		})
	}
}

func TestParseTradeErrors(t *testing.T) {
	t.Parallel()

	symbols := testSymbols(t)

	tests := []struct {
		name      string
		raw       string
		wantIs    error
		wantNotIs error
	}{
		{
			name:      "битый JSON",
			raw:       `{"stream":"btcusdt@trade","data":`,
			wantNotIs: errNotATrade,
		},
		{
			name:      "цена не число",
			raw:       `{"stream":"btcusdt@trade","data":{"e":"trade","s":"BTCUSDT","t":1,"p":"abc","q":"1","T":1757074530120}}`,
			wantNotIs: errNotATrade,
		},
		{
			name:   "нулевая цена не проходит доменную проверку",
			raw:    `{"stream":"btcusdt@trade","data":{"e":"trade","s":"BTCUSDT","t":1,"p":"0","q":"1","T":1757074530120}}`,
			wantIs: domain.ErrInvalidTrade,
		},
		{
			name:   "отрицательный объём не проходит доменную проверку",
			raw:    `{"stream":"btcusdt@trade","data":{"e":"trade","s":"BTCUSDT","t":1,"p":"1","q":"-1","T":1757074530120}}`,
			wantIs: domain.ErrInvalidTrade,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseTrade([]byte(tc.raw), symbols, SourceName)
			require.Error(t, err)
			if tc.wantIs != nil {
				require.ErrorIs(t, err, tc.wantIs)
			}
			if tc.wantNotIs != nil {
				require.NotErrorIs(t, err, tc.wantNotIs)
			}
		})
	}
}

func TestSymbolMapStreams(t *testing.T) {
	t.Parallel()

	symbols := []domain.Symbol{"BTC-USDT", "ETH-USDT", "BTC-USDC"}
	m, err := newSymbolMap(symbols)
	require.NoError(t, err)

	require.Equal(t, []string{"btcusdt@trade", "ethusdt@trade", "btcusdc@trade"}, m.streams(symbols))
}

func TestNewSymbolMapRejectsCollision(t *testing.T) {
	t.Parallel()

	// Разные доменные символы, дающие один тикер биржи: подписка на такой
	// список молча потеряла бы один из инструментов.
	_, err := newSymbolMap([]domain.Symbol{"BTC-USDT", "BTCU-SDT"})
	require.ErrorIs(t, err, domain.ErrInvalidSymbol)
}
