// Package domain содержит доменную модель конвейера рыночных данных.
//
// Пакет не зависит ни от Kafka, ни от PostgreSQL, ни от конкретной биржи:
// маппинг доменных символов на тикеры биржи живёт в адаптере биржи
// (internal/feed/binance), а сериализация — в адаптерах брокера и БД.
package domain

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidSymbol возвращается ParseSymbol; проверять через errors.Is.
var ErrInvalidSymbol = errors.New("invalid symbol")

// Symbol — нормализованный доменный символ инструмента вида BASE-QUOTE,
// например "BTC-USDT". Регистр всегда верхний, разделитель — дефис.
//
// Биржевые тикеры ("btcusdt") в домен не попадают: это деталь адаптера.
type Symbol string

// String реализует fmt.Stringer.
func (s Symbol) String() string { return string(s) }

const symbolSeparator = "-"

// ParseSymbol нормализует и валидирует доменный символ: обрезает пробелы,
// приводит к верхнему регистру и проверяет форму BASE-QUOTE.
//
// Нормализация вынесена в одну точку намеренно: символ приходит из конфига,
// из сообщений Kafka и из ответов биржи, и во всех трёх случаях он должен
// стать одним и тем же ключом партиционирования и одним и тем же значением
// первичного ключа в БД.
func ParseSymbol(s string) (Symbol, error) {
	normalized := strings.ToUpper(strings.TrimSpace(s))

	base, quote, ok := strings.Cut(normalized, symbolSeparator)
	if !ok {
		return "", fmt.Errorf("%w: %q: expected form BASE-QUOTE", ErrInvalidSymbol, s)
	}
	if !isAlphanumeric(base) || !isAlphanumeric(quote) {
		return "", fmt.Errorf("%w: %q: base and quote must be non-empty alphanumeric", ErrInvalidSymbol, s)
	}

	return Symbol(normalized), nil
}

// isAlphanumeric проверяет, что строка непустая и состоит только из
// латинских букв и цифр. Части символа сравниваются побайтово: после
// ToUpper допустимый алфавит тикеров — только ASCII.
func isAlphanumeric(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		default:
			return false
		}
	}
	return true
}
