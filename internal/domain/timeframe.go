package domain

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrUnknownTimeframe возвращается ParseTimeframe; проверять через errors.Is.
var ErrUnknownTimeframe = errors.New("unknown timeframe")

// Timeframe — длина окна агрегации. В MVP поддержаны только 1s и 1m
// этого достаточно, чтобы показать и «горячее» окно,
// и окно с редкими сделками.
type Timeframe string

const (
	TF1s Timeframe = "1s"
	TF1m Timeframe = "1m"
)

// String реализует fmt.Stringer.
func (tf Timeframe) String() string { return string(tf) }

// Duration возвращает длительность окна. Для неизвестного таймфрейма
// вернёт 0 — вызывающий код обязан получать Timeframe только через
// ParseTimeframe, поэтому отдельная ошибка здесь не нужна.
func (tf Timeframe) Duration() time.Duration {
	switch tf {
	case TF1s:
		return time.Second
	case TF1m:
		return time.Minute
	default:
		return 0
	}
}

// Truncate возвращает время начала окна, которому принадлежит t, в UTC.
//
// Опирается на time.Time.Truncate, который округляет вниз до кратного d
// относительно нулевого времени (1 января 1 года UTC). Смещение нулевого
// времени до Unix epoch — 62135596800 секунд, оно кратно и секунде, и
// минуте, поэтому для 1s и 1m результат совпадает с округлением вниз
// относительно epoch. Для таймфреймов вроде 1w это уже неверно — при
// расширении списка таймфреймов проверить заново.
//
// Перевод в UTC обязателен: Truncate работает с абсолютным временем, но
// монотонные часы нужно отбросить, а сравнения границ окон в агрегаторе
// идут по UTC.
func (tf Timeframe) Truncate(t time.Time) time.Time {
	d := tf.Duration()
	if d <= 0 {
		return t.UTC()
	}
	return t.UTC().Truncate(d)
}

// Timeframes возвращает поддерживаемые таймфреймы в порядке возрастания
// длительности. Функция, а не глобальная переменная: слайс-переменную
// вызывающий код мог бы изменить.
func Timeframes() []Timeframe {
	return []Timeframe{TF1s, TF1m}
}

// ParseTimeframe разбирает таймфрейм из конфига или из сообщения Kafka.
func ParseTimeframe(s string) (Timeframe, error) {
	tf := Timeframe(strings.ToLower(strings.TrimSpace(s)))
	if tf.Duration() <= 0 {
		return "", fmt.Errorf("%w: %q: supported are %s", ErrUnknownTimeframe, s, joinTimeframes(Timeframes()))
	}
	return tf, nil
}

func joinTimeframes(tfs []Timeframe) string {
	parts := make([]string, 0, len(tfs))
	for _, tf := range tfs {
		parts = append(parts, tf.String())
	}
	return strings.Join(parts, ", ")
}
