// Package aggregate собирает поток сделок в OHLCV-свечи по окнам времени.
//
// Пакет не знает ни про Kafka, ни про базу: на вход подаются доменные сделки,
// на выход отдаются доменные свечи. Время тоже приходит снаружи параметром —
// внутри не вызывается time.Now(), иначе логику закрытия окон нельзя было бы
// проверить тестами, не дожидаясь реальных секунд.
package aggregate

import (
	"time"

	"github.com/shopspring/decimal"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

// WindowKey однозначно определяет окно среди открытых.
//
// Ключ составной и плоский, а не вложенная мапа по символу и таймфрейму:
// проверка дедлайнов обходит все окна подряд, и одна итерация по плоской
// мапе проще и дешевле двух вложенных.
type WindowKey struct {
	Symbol    domain.Symbol
	Timeframe domain.Timeframe
}

// Window — открытое окно агрегации: свеча, которая ещё набирает сделки.
type Window struct {
	Key      WindowKey
	OpenTime time.Time

	Open   decimal.Decimal
	High   decimal.Decimal
	Low    decimal.Decimal
	Close  decimal.Decimal
	Volume decimal.Decimal

	TradeCount int64
}

// newWindow открывает окно первой сделкой.
//
// Все четыре цены равны цене этой сделки: окно из одной сделки — это свеча
// с нулевым размахом, а не свеча с нулевыми high и low.
func newWindow(key WindowKey, openTime time.Time, trade domain.Trade) *Window {
	return &Window{
		Key:        key,
		OpenTime:   openTime,
		Open:       trade.Price,
		High:       trade.Price,
		Low:        trade.Price,
		Close:      trade.Price,
		Volume:     trade.Quantity,
		TradeCount: 1,
	}
}

// apply добавляет сделку в окно.
//
// Open не трогается никогда: цена открытия окна задана первой сделкой и по
// определению неизменна. Close перезаписывается каждой сделкой, поэтому в
// закрытой свече им окажется цена последней.
func (w *Window) apply(trade domain.Trade) {
	if trade.Price.GreaterThan(w.High) {
		w.High = trade.Price
	}
	if trade.Price.LessThan(w.Low) {
		w.Low = trade.Price
	}

	w.Close = trade.Price
	w.Volume = w.Volume.Add(trade.Quantity)
	w.TradeCount++
}

// CloseTime возвращает границу окна: момент, с которого начинается следующее.
// Окно полуинтервально — [OpenTime, CloseTime).
func (w *Window) CloseTime() time.Time {
	return w.OpenTime.Add(w.Key.Timeframe.Duration())
}

// deadline — момент, начиная с которого окно можно закрывать.
//
// Это граница окна плюс grace period: время, которое даётся опоздавшим
// сделкам. Сеть и биржа доставляют события не строго по порядку, и без
// такой отсрочки сделка, отставшая на сотни миллисекунд, не попала бы
// в свою свечу.
func (w *Window) deadline(grace time.Duration) time.Time {
	return w.CloseTime().Add(grace)
}

// Candle превращает окно в готовую свечу.
func (w *Window) Candle() domain.Candle {
	return domain.Candle{
		Symbol:     w.Key.Symbol,
		Timeframe:  w.Key.Timeframe,
		OpenTime:   w.OpenTime,
		CloseTime:  w.CloseTime(),
		Open:       w.Open,
		High:       w.High,
		Low:        w.Low,
		Close:      w.Close,
		Volume:     w.Volume,
		TradeCount: w.TradeCount,
	}
}
