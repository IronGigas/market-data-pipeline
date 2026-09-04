package domain

import (
	"time"

	"github.com/shopspring/decimal"
)

// Candle — OHLCV-свеча за одно окно агрегации.
//
// Свеча создаётся только при наличии хотя бы одной сделки в интервале:
// пустые окна пропускаются, разрывы в ряду допустимы.
type Candle struct {
	Symbol    Symbol
	Timeframe Timeframe

	// OpenTime — начало окна в UTC, выровненное Timeframe.Truncate.
	// Вместе с Symbol и Timeframe образует первичный ключ в PostgreSQL,
	// что и делает запись свечи идемпотентной.
	OpenTime time.Time

	// CloseTime — OpenTime + Timeframe.Duration(); хранится явно, чтобы
	// потребителю свечи не требовалось знать длительность таймфрейма.
	CloseTime time.Time

	Open   decimal.Decimal
	High   decimal.Decimal
	Low    decimal.Decimal
	Close  decimal.Decimal
	Volume decimal.Decimal

	// TradeCount — число сделок, попавших в окно. Нужен, чтобы отличить
	// «сделок не было» от «объём округлился до нуля».
	TradeCount int64
}
