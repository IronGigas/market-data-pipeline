package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
)

// ErrInvalidTrade возвращается Trade.Validate; проверять через errors.Is.
var ErrInvalidTrade = errors.New("invalid trade")

// Trade — совершённая на бирже сделка, приведённая к доменному виду.
//
// Цена и объём — decimal, а не float64: на биржевых котировках двоичная
// плавающая точка даёт накопленную ошибку в сумме объёмов свечи.
type Trade struct {
	Symbol   Symbol
	TradeID  int64
	Price    decimal.Decimal
	Quantity decimal.Decimal

	// EventTime — время сделки по данным биржи (UTC), а не время получения.
	// Вся оконная логика агрегатора работает по нему.
	EventTime time.Time

	// Source — идентификатор источника ("binance"). Нужен, чтобы при
	// добавлении второго фида различать происхождение сделки.
	Source string
}

// Validate проверяет инварианты сделки перед публикацией в Kafka.
//
// Проверка живёт в домене, а не в адаптере биржи: те же инварианты должен
// соблюдать любой источник, включая консьюмер, читающий сделки из Kafka.
func (t Trade) Validate() error {
	switch {
	case t.Symbol == "":
		return fmt.Errorf("%w: empty symbol", ErrInvalidTrade)
	case t.Price.LessThanOrEqual(decimal.Zero):
		return fmt.Errorf("%w: %s: price must be positive, got %s", ErrInvalidTrade, t.Symbol, t.Price)
	case t.Quantity.LessThanOrEqual(decimal.Zero):
		return fmt.Errorf("%w: %s: quantity must be positive, got %s", ErrInvalidTrade, t.Symbol, t.Quantity)
	case t.EventTime.IsZero():
		return fmt.Errorf("%w: %s: empty event time", ErrInvalidTrade, t.Symbol)
	}
	return nil
}
