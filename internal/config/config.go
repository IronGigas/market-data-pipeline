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
	"time"

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

	// KafkaBrokers — адреса брокеров для первичного подключения. Остальные
	// узлы кластера клиент узнаёт из метаданных сам.
	KafkaBrokers []string

	TopicTrades string

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

	brokers, err := parseList(env("MDP_KAFKA_BROKERS", "localhost:9092"))
	if err != nil {
		return Ingestor{}, fmt.Errorf("%w: MDP_KAFKA_BROKERS: %w", ErrInvalidConfig, err)
	}

	topic, err := parseTopic(env("MDP_TOPIC_TRADES", "md.trades"))
	if err != nil {
		return Ingestor{}, fmt.Errorf("%w: MDP_TOPIC_TRADES: %w", ErrInvalidConfig, err)
	}

	return Ingestor{
		Symbols:      symbols,
		BinanceWSURL: wsURL,
		KafkaBrokers: brokers,
		TopicTrades:  topic,
		LogLevel:     level,
	}, nil
}

// parseList разбирает список значений через запятую, отбрасывая пустые.
func parseList(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))

	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			values = append(values, trimmed)
		}
	}

	if len(values) == 0 {
		return nil, errors.New("list is empty")
	}
	return values, nil
}

func parseTopic(raw string) (string, error) {
	topic := strings.TrimSpace(raw)
	if topic == "" {
		return "", errors.New("topic name is empty")
	}
	return topic, nil
}

// Aggregator — настройки сервиса aggregator.
type Aggregator struct {
	// Timeframes — таймфреймы, которые считаются параллельно из одного
	// потока сделок.
	Timeframes []domain.Timeframe

	KafkaBrokers  []string
	TopicTrades   string
	ConsumerGroup string

	// PostgresDSN — строка подключения к базе, куда пишутся свечи.
	PostgresDSN string

	// Grace — сколько ждать опоздавшие сделки после границы окна.
	Grace time.Duration

	// IdleTimeout — добавка к дедлайну для окон замолчавшего инструмента.
	IdleTimeout time.Duration

	LogLevel slog.Level
}

// LoadAggregator читает конфигурацию aggregator из окружения.
//
// Списка инструментов здесь нет намеренно: агрегатор считает окна по тем
// символам, которые реально пришли из топика, и знать их заранее ему незачем.
func LoadAggregator() (Aggregator, error) {
	timeframes, err := parseTimeframes(env("MDP_TIMEFRAMES", "1s,1m"))
	if err != nil {
		return Aggregator{}, err
	}

	level, err := parseLogLevel(env("MDP_LOG_LEVEL", "info"))
	if err != nil {
		return Aggregator{}, err
	}

	brokers, err := parseList(env("MDP_KAFKA_BROKERS", "localhost:9092"))
	if err != nil {
		return Aggregator{}, fmt.Errorf("%w: MDP_KAFKA_BROKERS: %w", ErrInvalidConfig, err)
	}

	topicTrades, err := parseTopic(env("MDP_TOPIC_TRADES", "md.trades"))
	if err != nil {
		return Aggregator{}, fmt.Errorf("%w: MDP_TOPIC_TRADES: %w", ErrInvalidConfig, err)
	}

	group := strings.TrimSpace(env("MDP_CONSUMER_GROUP", "md-aggregator"))
	if group == "" {
		return Aggregator{}, fmt.Errorf("%w: MDP_CONSUMER_GROUP is empty", ErrInvalidConfig)
	}

	dsn := strings.TrimSpace(env("MDP_POSTGRES_DSN",
		"postgres://marketdata:marketdata@localhost:5432/marketdata?sslmode=disable"))
	if dsn == "" {
		return Aggregator{}, fmt.Errorf("%w: MDP_POSTGRES_DSN is empty", ErrInvalidConfig)
	}

	grace, err := parseDuration(env("MDP_GRACE_PERIOD", "2s"))
	if err != nil {
		return Aggregator{}, fmt.Errorf("%w: MDP_GRACE_PERIOD: %w", ErrInvalidConfig, err)
	}

	idleTimeout, err := parseDuration(env("MDP_IDLE_TIMEOUT", "3s"))
	if err != nil {
		return Aggregator{}, fmt.Errorf("%w: MDP_IDLE_TIMEOUT: %w", ErrInvalidConfig, err)
	}

	return Aggregator{
		Timeframes:    timeframes,
		KafkaBrokers:  brokers,
		TopicTrades:   topicTrades,
		ConsumerGroup: group,
		PostgresDSN:   dsn,
		Grace:         grace,
		IdleTimeout:   idleTimeout,
		LogLevel:      level,
	}, nil
}

// parseTimeframes разбирает список таймфреймов через запятую.
func parseTimeframes(raw string) ([]domain.Timeframe, error) {
	parts := strings.Split(raw, ",")
	timeframes := make([]domain.Timeframe, 0, len(parts))
	seen := make(map[domain.Timeframe]struct{}, len(parts))

	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}

		tf, err := domain.ParseTimeframe(part)
		if err != nil {
			return nil, fmt.Errorf("%w: MDP_TIMEFRAMES: %w", ErrInvalidConfig, err)
		}
		// Дубликат означал бы два одинаковых окна на инструмент и двойную
		// запись одной и той же свечи.
		if _, dup := seen[tf]; dup {
			return nil, fmt.Errorf("%w: MDP_TIMEFRAMES: duplicate timeframe %q", ErrInvalidConfig, tf)
		}

		seen[tf] = struct{}{}
		timeframes = append(timeframes, tf)
	}

	if len(timeframes) == 0 {
		return nil, fmt.Errorf("%w: MDP_TIMEFRAMES is empty", ErrInvalidConfig)
	}
	return timeframes, nil
}

// parseDuration разбирает длительность в формате time.ParseDuration.
// Ноль и отрицательные значения отвергаются: они означали бы окно без
// grace period, что почти наверняка опечатка, а не намерение.
func parseDuration(raw string) (time.Duration, error) {
	d, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("%q: %w", raw, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%q: must be positive", raw)
	}
	return d, nil
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
