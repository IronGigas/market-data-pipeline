package domain_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

func TestParseSymbol(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    domain.Symbol
		wantErr bool
	}{
		{name: "канонический вид", in: "BTC-USDT", want: "BTC-USDT"},
		{name: "нижний регистр нормализуется", in: "btc-usdt", want: "BTC-USDT"},
		{name: "пробелы обрезаются", in: "  ETH-USDT\n", want: "ETH-USDT"},
		{name: "цифры в тикере допустимы", in: "1INCH-USDT", want: "1INCH-USDT"},
		{name: "без разделителя", in: "BTCUSDT", wantErr: true},
		{name: "пустая база", in: "-USDT", wantErr: true},
		{name: "пустая котировка", in: "BTC-", wantErr: true},
		{name: "два разделителя", in: "BTC-USD-T", wantErr: true},
		{name: "пробел внутри", in: "BTC -USDT", wantErr: true},
		{name: "пустая строка", in: "", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := domain.ParseSymbol(tc.in)
			if tc.wantErr {
				require.ErrorIs(t, err, domain.ErrInvalidSymbol)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestSymbolBaseQuote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		in        domain.Symbol
		wantBase  string
		wantQuote string
	}{
		{name: "BTC-USDT", in: "BTC-USDT", wantBase: "BTC", wantQuote: "USDT"},
		{name: "BTC-USDC", in: "BTC-USDC", wantBase: "BTC", wantQuote: "USDC"},
		{name: "невалидный символ даёт пустые части", in: "BTCUSDT"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.wantBase, tc.in.Base())
			require.Equal(t, tc.wantQuote, tc.in.Quote())
		})
	}
}
