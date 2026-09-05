// Package binance реализует feed.Feed поверх публичного WebSocket Binance.
package binance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

// SourceName попадает в поле Source каждой сделки.
const SourceName = "binance"

// errFatal помечает ошибки, при которых переподключение бессмысленно:
// повтор даст тот же результат, и правильнее остановиться, чем крутить
// цикл реконнектов вхолостую.
var errFatal = errors.New("fatal feed error")

// Значения по умолчанию для Config. Вынесены в константы, чтобы конфиг
// можно было заполнить частично, а причины выбора описать в одном месте.
const (
	// Раз в ~3 минуты Binance шлёт ping и ждёт pong; библиотека отвечает
	// автоматически. Но молчащее соединение бывает живым с точки зрения TCP,
	// поэтому 90 секунд без единого сообщения считаем обрывом: это заметно
	// больше периода ping и заметно меньше времени, за которое отставание
	// от рынка станет бессмысленным.
	defaultStaleTimeout = 90 * time.Second

	defaultDialTimeout = 10 * time.Second

	defaultMinBackoff = time.Second
	defaultMaxBackoff = 30 * time.Second

	// Сессия, прожившая дольше этого времени, считается успешной, и счётчик
	// попыток сбрасывается. Без сброса редкие обрывы раз в час постепенно
	// довели бы паузу до потолка.
	defaultStableAfter = time.Minute

	// Сообщение о сделке — сотни байт. Лимит защищает от неограниченного
	// выделения памяти при неожиданно большом кадре.
	defaultReadLimit = 1 << 20
)

// Config — параметры клиента. Обязательны URL, Symbols и Logger, остальное
// имеет разумные умолчания.
type Config struct {
	// URL — базовый адрес комбинированного потока, без списка стримов.
	URL     string
	Symbols []domain.Symbol
	Logger  *slog.Logger

	DialTimeout  time.Duration
	StaleTimeout time.Duration
	MinBackoff   time.Duration
	MaxBackoff   time.Duration
	StableAfter  time.Duration
	ReadLimit    int64
}

// Stats — счётчики фида на момент вызова.
type Stats struct {
	Received   int64 // разобранные сделки, переданные в handler
	Duplicates int64 // отброшено дедупликацией после реконнекта
	Skipped    int64 // сообщения не о сделке или по чужому инструменту
	Failed     int64 // сообщения, которые не удалось разобрать
	Reconnects int64
}

// Client читает сделки из комбинированного потока Binance.
//
// Клиент рассчитан на один вызов Run: состояние дедупликации живёт в горутине
// чтения без синхронизации. Счётчики читаются снаружи и потому атомарные.
type Client struct {
	url     string
	symbols []domain.Symbol
	mapper  *symbolMap
	log     *slog.Logger

	dialTimeout  time.Duration
	staleTimeout time.Duration
	minBackoff   time.Duration
	maxBackoff   time.Duration
	stableAfter  time.Duration
	readLimit    int64

	received   atomic.Int64
	duplicates atomic.Int64
	skipped    atomic.Int64
	failed     atomic.Int64
	reconnects atomic.Int64

	// lastTradeID — последний виденный TradeID по каждому инструменту.
	// Читается и пишется только горутиной Run.
	lastTradeID map[domain.Symbol]int64
}

// New собирает клиента и проверяет параметры. Зависимости передаются снаружи:
// клиент не создаёт логгер и не читает окружение.
func New(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, errors.New("binance: empty URL")
	}
	if len(cfg.Symbols) == 0 {
		return nil, errors.New("binance: no symbols to subscribe")
	}
	if cfg.Logger == nil {
		return nil, errors.New("binance: logger is required")
	}

	mapper, err := newSymbolMap(cfg.Symbols)
	if err != nil {
		return nil, fmt.Errorf("binance: build symbol map: %w", err)
	}

	readLimit := cfg.ReadLimit
	if readLimit <= 0 {
		readLimit = defaultReadLimit
	}

	return &Client{
		url:          cfg.URL,
		symbols:      cfg.Symbols,
		mapper:       mapper,
		log:          cfg.Logger,
		dialTimeout:  orDefault(cfg.DialTimeout, defaultDialTimeout),
		staleTimeout: orDefault(cfg.StaleTimeout, defaultStaleTimeout),
		minBackoff:   orDefault(cfg.MinBackoff, defaultMinBackoff),
		maxBackoff:   orDefault(cfg.MaxBackoff, defaultMaxBackoff),
		stableAfter:  orDefault(cfg.StableAfter, defaultStableAfter),
		readLimit:    readLimit,
		lastTradeID:  make(map[domain.Symbol]int64, len(cfg.Symbols)),
	}, nil
}

// Stats возвращает снимок счётчиков. Безопасен для вызова из другой горутины.
func (c *Client) Stats() Stats {
	return Stats{
		Received:   c.received.Load(),
		Duplicates: c.duplicates.Load(),
		Skipped:    c.skipped.Load(),
		Failed:     c.failed.Load(),
		Reconnects: c.reconnects.Load(),
	}
}

// Run подключается к бирже и передаёт сделки в handler, переподключаясь при
// обрывах, пока не отменён ctx.
func (c *Client) Run(ctx context.Context, handler func(domain.Trade) error) error {
	streamURL, err := c.streamURL()
	if err != nil {
		return err
	}

	c.log.Info("feed starting",
		slog.String("source", SourceName),
		slog.Int("symbols", len(c.symbols)),
		slog.String("url", streamURL))

	attempt := 0
	for {
		if ctx.Err() != nil {
			return nil
		}

		attempt++
		startedAt := time.Now()
		err := c.session(ctx, streamURL, attempt, handler)

		// Отмена контекста — штатная остановка, а не сбой соединения.
		if ctx.Err() != nil {
			c.log.Info("feed stopped", slog.String("source", SourceName))
			return nil
		}
		if errors.Is(err, errFatal) {
			return err
		}

		// Долгая сессия означает, что проблема была разовой: начинаем отсчёт
		// пауз заново, иначе редкие обрывы постепенно накрутят потолок.
		if time.Since(startedAt) >= c.stableAfter {
			attempt = 1
		}

		delay := c.backoff(attempt)
		c.reconnects.Add(1)
		c.log.Warn("feed disconnected, reconnecting",
			slog.String("error", err.Error()),
			slog.Int("attempt", attempt),
			slog.Duration("retry_in", delay))

		select {
		case <-ctx.Done():
			c.log.Info("feed stopped", slog.String("source", SourceName))
			return nil
		case <-time.After(delay):
		}
	}
}

// session держит одно соединение до первой ошибки чтения.
func (c *Client) session(ctx context.Context, streamURL string, attempt int, handler func(domain.Trade) error) error {
	dialCtx, cancel := context.WithTimeout(ctx, c.dialTimeout)
	defer cancel()

	conn, _, err := websocket.Dial(dialCtx, streamURL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	// CloseNow, а не Close: рвём соединение без обмена close-кадрами. Мы уже
	// уходим на переподключение, ждать вежливого ответа биржи незачем.
	defer conn.CloseNow() //nolint:errcheck // соединение всё равно закрывается

	conn.SetReadLimit(c.readLimit)
	c.log.Info("feed connected",
		slog.String("source", SourceName),
		slog.Int("attempt", attempt))

	for {
		// Таймаут на каждое чтение: если биржа замолчала, соединение
		// считается мёртвым и сессия завершается ошибкой.
		readCtx, cancelRead := context.WithTimeout(ctx, c.staleTimeout)
		_, raw, err := conn.Read(readCtx)
		cancelRead()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}

		if err := c.handleMessage(raw, handler); err != nil {
			return err
		}
	}
}

// handleMessage разбирает одно сообщение и отдаёт сделку в handler.
func (c *Client) handleMessage(raw []byte, handler func(domain.Trade) error) error {
	trade, err := parseTrade(raw, c.mapper, SourceName)
	switch {
	case errors.Is(err, errNotATrade):
		c.skipped.Add(1)
		return nil
	case err != nil:
		// Одно нечитаемое сообщение не повод рвать соединение: биржа может
		// прислать служебное событие неизвестного нам вида.
		c.failed.Add(1)
		c.log.Warn("skip malformed message", slog.String("error", err.Error()))
		return nil
	}

	if c.isDuplicate(trade) {
		c.duplicates.Add(1)
		c.log.Debug("duplicate trade dropped",
			slog.String("symbol", trade.Symbol.String()),
			slog.Int64("trade_id", trade.TradeID))
		return nil
	}

	c.received.Add(1)
	if err := handler(trade); err != nil {
		return fmt.Errorf("%w: handler: %w", errFatal, err)
	}
	return nil
}

// isDuplicate отсекает повторы, приходящие после переподключения: биржа может
// прислать хвост уже виденных сделок. TradeID внутри инструмента монотонно
// растёт, поэтому достаточно помнить последний.
func (c *Client) isDuplicate(trade domain.Trade) bool {
	last, seen := c.lastTradeID[trade.Symbol]
	if seen && trade.TradeID <= last {
		return true
	}
	c.lastTradeID[trade.Symbol] = trade.TradeID
	return false
}

// streamURL добавляет к базовому адресу список стримов:
//
//	wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade
func (c *Client) streamURL() (string, error) {
	u, err := url.Parse(c.url)
	if err != nil {
		return "", fmt.Errorf("%w: parse url %q: %w", errFatal, c.url, err)
	}

	query := u.Query()
	query.Set("streams", strings.Join(c.mapper.streams(c.symbols), "/"))
	u.RawQuery = query.Encode()

	return u.String(), nil
}

// backoff возвращает паузу перед попыткой номер attempt: 1s, 2s, 4s, ...
// с потолком maxBackoff и джиттером ±20%.
//
// Джиттер нужен даже одиночному клиенту: без него после обрыва на стороне
// биржи все переподключения выстраиваются в одну и ту же сетку моментов.
func (c *Client) backoff(attempt int) time.Duration {
	delay := c.minBackoff
	for i := 1; i < attempt && delay < c.maxBackoff; i++ {
		delay *= 2
	}
	if delay > c.maxBackoff {
		delay = c.maxBackoff
	}

	jitter := 0.8 + 0.4*rand.Float64() //nolint:gosec // не криптография
	return time.Duration(float64(delay) * jitter)
}

func orDefault(v, fallback time.Duration) time.Duration {
	if v <= 0 {
		return fallback
	}
	return v
}
