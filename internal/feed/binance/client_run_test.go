package binance

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

// tradeJSON собирает сообщение комбинированного потока с заданным TradeID.
func tradeJSON(id int64) string {
	return `{"stream":"btcusdt@trade","data":{"e":"trade","E":1757074530123,` +
		`"s":"BTCUSDT","t":` + strconv.FormatInt(id, 10) +
		`,"p":"64250.15","q":"0.001","T":1757074530120}}`
}

// TestRunReconnectsAndDeduplicates поднимает локальный WebSocket-сервер,
// который обрывает первое соединение и повторяет уже отданную сделку во
// втором. Проверяются три требования сразу: переподключение после обрыва,
// отсев повторов после реконнекта и штатный выход по отмене контекста.
//
// Сервер локальный и живёт в том же процессе: тест не ходит в сеть и не
// зависит от доступности биржи.
func TestRunReconnectsAndDeduplicates(t *testing.T) {
	t.Parallel()

	var connections atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow() //nolint:errcheck // соединение всё равно закрывается

		// Первое соединение отдаёт сделки 1 и 2 и рвётся. Второе начинает
		// с уже отданной сделки 2 — ровно так ведёт себя биржа при обрыве.
		ids := []int64{2, 3}
		if connections.Add(1) == 1 {
			ids = []int64{1, 2}
		}

		for _, id := range ids {
			if err := conn.Write(r.Context(), websocket.MessageText, []byte(tradeJSON(id))); err != nil {
				return
			}
		}

		if connections.Load() == 1 {
			return // обрыв соединения
		}
		<-r.Context().Done() // второе соединение держим открытым
	}))
	defer srv.Close()

	client, err := New(Config{
		URL:     "ws" + strings.TrimPrefix(srv.URL, "http"),
		Symbols: []domain.Symbol{"BTC-USDT"},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		// Паузы сжаты до миллисекунд: проверяется факт переподключения,
		// а не длительность backoff (для неё есть TestBackoff).
		MinBackoff:   10 * time.Millisecond,
		MaxBackoff:   10 * time.Millisecond,
		StableAfter:  time.Hour,
		DialTimeout:  5 * time.Second,
		StaleTimeout: 5 * time.Second,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	trades := make(chan domain.Trade, 8)
	done := make(chan error, 1)
	go func() {
		done <- client.Run(ctx, func(trade domain.Trade) error {
			trades <- trade
			return nil
		})
	}()

	var got []int64
	deadline := time.After(15 * time.Second)
	for len(got) < 3 {
		select {
		case trade := <-trades:
			got = append(got, trade.TradeID)
		case <-deadline:
			t.Fatalf("получено %v, ожидались сделки 1, 2, 3", got)
		}
	}

	cancel()

	select {
	case err := <-done:
		// Отмена контекста — штатная остановка, а не ошибка.
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run не завершился после отмены контекста")
	}

	require.Equal(t, []int64{1, 2, 3}, got, "повтор сделки 2 должен быть отброшен")

	stats := client.Stats()
	require.Equal(t, int64(3), stats.Received)
	require.Equal(t, int64(1), stats.Duplicates)
	require.GreaterOrEqual(t, stats.Reconnects, int64(1))
}

// TestRunStopsOnHandlerError проверяет, что ошибка обработчика считается
// неустранимой: переподключение её не исправит, и Run обязан вернуть ошибку,
// а не крутить цикл реконнектов.
func TestRunStopsOnHandlerError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow() //nolint:errcheck // соединение всё равно закрывается

		_ = conn.Write(r.Context(), websocket.MessageText, []byte(tradeJSON(1)))
		<-r.Context().Done()
	}))
	defer srv.Close()

	client, err := New(Config{
		URL:          "ws" + strings.TrimPrefix(srv.URL, "http"),
		Symbols:      []domain.Symbol{"BTC-USDT"},
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		MinBackoff:   10 * time.Millisecond,
		MaxBackoff:   10 * time.Millisecond,
		DialTimeout:  5 * time.Second,
		StaleTimeout: 5 * time.Second,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	handlerErr := io.ErrClosedPipe
	err = client.Run(ctx, func(domain.Trade) error { return handlerErr })

	require.ErrorIs(t, err, handlerErr)
	require.ErrorIs(t, err, errFatal)
}
