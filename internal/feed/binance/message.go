package binance

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

// errNotATrade — сообщение корректно, но это не сделка: ответ на подписку,
// событие другого типа или инструмент, на который мы не подписаны. Такие
// сообщения пропускаются без шума в логах.
var errNotATrade = errors.New("not a trade message")

// eventTypeTrade — значение поля "e" у события сделки.
const eventTypeTrade = "trade"

// combinedMessage — обёртка комбинированного потока:
//
//	{"stream":"btcusdt@trade","data":{ ... }}
//
// Поле data оставлено сырым: сначала нужно понять тип события, и только
// потом разбирать его как сделку.
type combinedMessage struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// tradeEvent — событие сделки Binance. Однобуквенные имена полей — формат
// биржи, а не наш выбор.
//
// Цена и количество приходят строками, и разбираются в decimal напрямую,
// минуя float64: JSON-число превратилось бы в двоичную дробь и потеряло
// последние знаки ещё до попадания в домен.
// Поля E и e различаются только регистром, и оба обязаны быть объявлены:
// encoding/json сопоставляет ключи регистронезависимо, если точного
// совпадения нет. Убери EventSentTime — и число из "E" попытается лечь
// в строковое поле "e", уронив разбор всего сообщения.
type tradeEvent struct {
	EventType string `json:"e"`
	Symbol    string `json:"s"`
	TradeID   int64  `json:"t"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`

	// TradeTime — время совершения сделки, миллисекунды от epoch.
	// Именно оно становится EventTime и определяет окно агрегации.
	TradeTime int64 `json:"T"`

	// EventSentTime — время отправки события биржей. Не используется:
	// оно отражает задержку доставки, а не момент сделки. Объявлено ради
	// точного совпадения ключа (см. комментарий выше).
	EventSentTime int64 `json:"E"`
}

// parseTrade разбирает сырое сообщение комбинированного потока в доменную
// сделку. Возвращает errNotATrade для сообщений, которые не являются
// сделкой по подписанному инструменту.
func parseTrade(raw []byte, symbols *symbolMap, source string) (domain.Trade, error) {
	var envelope combinedMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return domain.Trade{}, fmt.Errorf("decode envelope: %w", err)
	}
	// Ответы на служебные команды приходят без поля data.
	if len(envelope.Data) == 0 {
		return domain.Trade{}, errNotATrade
	}

	var event tradeEvent
	if err := json.Unmarshal(envelope.Data, &event); err != nil {
		return domain.Trade{}, fmt.Errorf("decode trade event: %w", err)
	}
	if event.EventType != eventTypeTrade {
		return domain.Trade{}, errNotATrade
	}

	symbol, ok := symbols.symbol(event.Symbol)
	if !ok {
		return domain.Trade{}, errNotATrade
	}

	price, err := decimal.NewFromString(event.Price)
	if err != nil {
		return domain.Trade{}, fmt.Errorf("parse price %q: %w", event.Price, err)
	}

	quantity, err := decimal.NewFromString(event.Quantity)
	if err != nil {
		return domain.Trade{}, fmt.Errorf("parse quantity %q: %w", event.Quantity, err)
	}

	trade := domain.Trade{
		Symbol:   symbol,
		TradeID:  event.TradeID,
		Price:    price,
		Quantity: quantity,
		// Берётся T (время сделки), а не E (время отправки события):
		// оконная агрегация обязана идти по времени события на бирже.
		EventTime: time.UnixMilli(event.TradeTime).UTC(),
		Source:    source,
	}

	if err := trade.Validate(); err != nil {
		return domain.Trade{}, fmt.Errorf("trade %d: %w", event.TradeID, err)
	}
	return trade, nil
}
