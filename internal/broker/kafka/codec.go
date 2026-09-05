// Package kafka содержит адаптеры Kafka: формат сообщений, продюсер и консьюмер.
package kafka

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/shopspring/decimal"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

// timeLayout — RFC3339 с миллисекундами: 2026-09-03T10:15:30.123Z
//
// Фиксированные три знака после точки важнее краткости: time.RFC3339Nano
// отбрасывает незначащие нули, и одна и та же секунда выглядела бы то как
// .120Z, то как .12Z. Ровная ширина поля читается глазами в Kafka UI и не
// сбивает сортировку строк.
const timeLayout = "2006-01-02T15:04:05.000Z07:00"

// tradeMessage — представление сделки в топике md.trades.
//
// Цена и объём — строки, а не JSON-числа. Числовой литерал любой JSON-парсер
// разберёт в float64 и потеряет последние знаки; строка доходит до decimal
// без изменений. Ровно та же причина, по которой Binance отдаёт их строками.
type tradeMessage struct {
	Symbol    string `json:"symbol"`
	TradeID   int64  `json:"trade_id"`
	Price     string `json:"price"`
	Quantity  string `json:"quantity"`
	EventTime string `json:"event_time"`
	Source    string `json:"source"`
}

// EncodeTrade сериализует сделку для публикации в Kafka.
func EncodeTrade(trade domain.Trade) ([]byte, error) {
	msg := tradeMessage{
		Symbol:    trade.Symbol.String(),
		TradeID:   trade.TradeID,
		Price:     trade.Price.String(),
		Quantity:  trade.Quantity.String(),
		EventTime: trade.EventTime.UTC().Format(timeLayout),
		Source:    trade.Source,
	}

	raw, err := json.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("encode trade: %w", err)
	}
	return raw, nil
}

// DecodeTrade разбирает сообщение из топика md.trades.
//
// Декодер живёт рядом с кодировщиком намеренно: формат сообщения — общий
// контракт двух сервисов, и держать его в одном файле дешевле, чем ловить
// расхождение между ними в рантайме.
func DecodeTrade(raw []byte) (domain.Trade, error) {
	var msg tradeMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return domain.Trade{}, fmt.Errorf("decode trade: %w", err)
	}

	symbol, err := domain.ParseSymbol(msg.Symbol)
	if err != nil {
		return domain.Trade{}, fmt.Errorf("decode trade: %w", err)
	}

	price, err := decimal.NewFromString(msg.Price)
	if err != nil {
		return domain.Trade{}, fmt.Errorf("decode trade: price %q: %w", msg.Price, err)
	}

	quantity, err := decimal.NewFromString(msg.Quantity)
	if err != nil {
		return domain.Trade{}, fmt.Errorf("decode trade: quantity %q: %w", msg.Quantity, err)
	}

	eventTime, err := time.Parse(time.RFC3339, msg.EventTime)
	if err != nil {
		return domain.Trade{}, fmt.Errorf("decode trade: event_time %q: %w", msg.EventTime, err)
	}

	trade := domain.Trade{
		Symbol:   symbol,
		TradeID:  msg.TradeID,
		Price:    price,
		Quantity: quantity,
		// UTC принудительно: RFC3339 допускает смещение, а вся оконная
		// логика сравнивает моменты в UTC.
		EventTime: eventTime.UTC(),
		Source:    msg.Source,
	}

	if err := trade.Validate(); err != nil {
		return domain.Trade{}, fmt.Errorf("decode trade: %w", err)
	}
	return trade, nil
}

// TradeKey возвращает ключ партиционирования для сделки.
//
// Ключ — доменный символ: Kafka гарантирует порядок внутри партиции, а нам
// нужен порядок внутри инструмента. Плата за это — неравномерная загрузка
// партиций: по BTC-USDT сделок на порядки больше, чем по BTC-USDC.
func TradeKey(trade domain.Trade) []byte {
	return []byte(trade.Symbol)
}
