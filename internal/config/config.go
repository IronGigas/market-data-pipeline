// Package config читает настройки сервисов из переменных окружения.
//
// Файлов конфигурации нет намеренно: сервисы запускаются и локально, и в
// контейнере, а окружение — единственный способ, работающий одинаково в
// обоих случаях. Невалидное значение приводит к отказу на старте, а не к
// молчаливой подстановке умолчания: тихий дефолт превращает опечатку в
// переменной в загадочное поведение через час работы.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

// ErrInvalidConfig возвращается загрузчиками; проверять через errors.Is.
var ErrInvalidConfig = errors.New("invalid config")

// Ingestor — настройки сервиса ingestor.
type Ingestor struct {
	// Symbols — инструменты, на которые подписывается фид. Порядок сохраняется
	// из переменной окружения: он влияет только на вид URL подписки.
	Symbols []domain.Symbol

	// BinanceWSURL — базовый адрес комбинированного потока, без списка стримов.
	// Список формирует адаптер биржи из Symbols.
	BinanceWSURL string

	LogLevel slog.Level
}

// LoadIngestor читает конфигурацию ingestor из окружения.
func LoadIngestor() (Ingestor, error) {
	symbols, err := parseSymbols(env("MDP_SYMBOLS", "BTC-USDT,ETH-USDT,BTC-USDC"))
	if err != nil {
		return Ingestor{}, err
	}

	level, err := parseLogLevel(env("MDP_LOG_LEVEL", "info"))
	if err != nil {
		return Ingestor{}, err
	}

	wsURL := strings.TrimSpace(env("MDP_BINANCE_WS_URL", "wss://stream.binance.com:9443/stream"))
	if wsURL == "" {
		return Ingestor{}, fmt.Errorf("%w: MDP_BINANCE_WS_URL is empty", ErrInvalidConfig)
	}

	return Ingestor{
		Symbols:      symbols,
		BinanceWSURL: wsURL,
		LogLevel:     level,
	}, nil
}

// env возвращает значение переменной окружения или значение по умолчанию.
// Пустая строка считается «не задано»: в docker compose переменную легко
// объявить без значения, и такой случай должен вести себя как её отсутствие.
func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return fallback
}

// parseSymbols разбирает список инструментов через запятую и нормализует их
// доменным парсером — так конфиг не может протащить в систему символ в форме,
// отличной от той, что станет ключом партиционирования и первичным ключом.
func parseSymbols(raw string) ([]domain.Symbol, error) {
	parts := strings.Split(raw, ",")
	symbols := make([]domain.Symbol, 0, len(parts))
	seen := make(map[domain.Symbol]struct{}, len(parts))

	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}

		symbol, err := domain.ParseSymbol(part)
		if err != nil {
			return nil, fmt.Errorf("%w: MDP_SYMBOLS: %w", ErrInvalidConfig, err)
		}
		// Дубликат в списке дал бы вторую подписку на тот же стрим и удвоил
		// поток сделок — это ошибка конфигурации, а не безобидный повтор.
		if _, dup := seen[symbol]; dup {
			return nil, fmt.Errorf("%w: MDP_SYMBOLS: duplicate symbol %q", ErrInvalidConfig, symbol)
		}

		seen[symbol] = struct{}{}
		symbols = append(symbols, symbol)
	}

	if len(symbols) == 0 {
		return nil, fmt.Errorf("%w: MDP_SYMBOLS is empty", ErrInvalidConfig)
	}
	return symbols, nil
}

func parseLogLevel(raw string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("%w: MDP_LOG_LEVEL: %q: supported are debug, info, warn, error", ErrInvalidConfig, raw)
	}
}
