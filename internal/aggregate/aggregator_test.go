package aggregate

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

// baseTime — начало минуты, от которого отсчитываются все сценарии.
var baseTime = time.Date(2026, 9, 3, 10, 15, 0, 0, time.UTC)

const (
	testGrace = 2 * time.Second
	testIdle  = 3 * time.Second
)

func newTestAggregator(t *testing.T, timeframes ...domain.Timeframe) *Aggregator {
	t.Helper()

	if len(timeframes) == 0 {
		timeframes = []domain.Timeframe{domain.TF1m}
	}

	a, err := New(Config{
		Timeframes:  timeframes,
		Grace:       testGrace,
		IdleTimeout: testIdle,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	return a
}

// addLive добавляет сделку так, будто часы сервиса совпадают с временем
// биржи, — это нормальный режим работы. Сценарий разбора накопленного,
// где шкалы расходятся, проверяется отдельно.
func addLive(a *Aggregator, tr domain.Trade) {
	a.Add(tr, tr.EventTime)
}

// trade собирает сделку по инструменту в заданный момент.
func trade(symbol domain.Symbol, price, quantity string, eventTime time.Time) domain.Trade {
	return domain.Trade{
		Symbol:    symbol,
		TradeID:   eventTime.UnixMilli(),
		Price:     decimal.RequireFromString(price),
		Quantity:  decimal.RequireFromString(quantity),
		EventTime: eventTime,
		Source:    "binance",
	}
}

func TestNewValidatesConfig(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	_, err := New(Config{Logger: logger})
	require.Error(t, err, "без таймфреймов агрегатор бессмыслен")

	_, err = New(Config{Timeframes: []domain.Timeframe{domain.TF1m}})
	require.Error(t, err, "логгер обязателен")
}

// TestCloseByWatermark — основной сценарий: окно закрывает время биржи,
// а не системные часы. Обратите внимание, что now в Expired стоит на месте.
func TestCloseByWatermark(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t)

	addLive(a, trade("BTC-USDT", "100", "1", baseTime.Add(10*time.Second)))
	addLive(a, trade("BTC-USDT", "110", "2", baseTime.Add(30*time.Second)))

	// Watermark внутри окна — закрывать нечего.
	require.Empty(t, a.Expired(baseTime))

	// Сделка следующей минуты открывает второе окно и уводит watermark
	// за дедлайн первого (10:16:00 + grace 2s = 10:16:02).
	addLive(a, trade("BTC-USDT", "120", "1", baseTime.Add(65*time.Second)))
	require.Equal(t, 2, a.Stats().OpenWindows, "оба окна открыты одновременно")

	closed := a.Expired(baseTime)
	require.Len(t, closed, 1, "закрылось только первое окно")

	candle := closed[0]
	require.True(t, candle.OpenTime.Equal(baseTime))
	require.True(t, candle.CloseTime.Equal(baseTime.Add(time.Minute)))
	require.Equal(t, "100", candle.Open.String())
	require.Equal(t, "110", candle.High.String())
	require.Equal(t, "100", candle.Low.String())
	require.Equal(t, "110", candle.Close.String())
	require.Equal(t, "3", candle.Volume.String())
	require.Equal(t, int64(2), candle.TradeCount)

	require.Equal(t, 1, a.Stats().OpenWindows, "окно 10:16 продолжает жить")
}

// TestGraceAcceptsLateTrade — то, ради чего окон на ключ несколько.
// Сделка приходит после границы окна, но внутри grace period, и обязана
// попасть в свою свечу, а не быть отброшенной.
func TestGraceAcceptsLateTrade(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t, domain.TF1s)

	// Окно 10:15:00.
	addLive(a, trade("BTC-USDT", "100", "1", baseTime.Add(500*time.Millisecond)))

	// Сделка следующей секунды: окно 10:15:00 остаётся открытым,
	// рядом открывается 10:15:01.
	addLive(a, trade("BTC-USDT", "200", "1", baseTime.Add(1200*time.Millisecond)))
	require.Equal(t, 2, a.Stats().OpenWindows)

	// Отставшая сделка из первой секунды — её окно ещё живо (дедлайн
	// 10:15:01 + 2s = 10:15:03), значит она принимается.
	addLive(a, trade("BTC-USDT", "150", "3", baseTime.Add(800*time.Millisecond)))
	require.Equal(t, int64(0), a.Stats().Late, "сделка внутри grace не опоздала")

	// Уводим watermark за дедлайн первого окна.
	addLive(a, trade("BTC-USDT", "210", "1", baseTime.Add(3500*time.Millisecond)))

	closed := a.Expired(baseTime)
	require.Len(t, closed, 1)

	candle := closed[0]
	require.True(t, candle.OpenTime.Equal(baseTime))
	require.Equal(t, int64(2), candle.TradeCount, "отставшая сделка учтена")
	require.Equal(t, "150", candle.High.String())
	require.Equal(t, "4", candle.Volume.String())
}

// TestCloseByIdleTimeout — страховка для замолчавшего инструмента: сделок
// больше нет, watermark стоит, и окно закрывают системные часы.
func TestCloseByIdleTimeout(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t)

	addLive(a, trade("BTC-USDC", "100", "1", baseTime.Add(10*time.Second)))

	// Дедлайн окна: 10:16:00 + grace 2s = 10:16:02.
	// Порог простоя: дедлайн + idle 3s = 10:16:05.
	require.Empty(t, a.Expired(baseTime.Add(64*time.Second)), "10:16:04 — рано")

	closed := a.Expired(baseTime.Add(65 * time.Second))
	require.Len(t, closed, 1, "10:16:05 — пора")
	require.True(t, closed[0].OpenTime.Equal(baseTime))
	require.Equal(t, int64(1), closed[0].TradeCount)
}

// TestLateTradeDroppedAfterClose проверяет, что после закрытия окна сделка
// из него отбрасывается, а не пересчитывает свечу и не открывает окно заново.
func TestLateTradeDroppedAfterClose(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t)

	addLive(a, trade("BTC-USDT", "100", "1", baseTime.Add(10*time.Second)))

	closed := a.Expired(baseTime.Add(70 * time.Second))
	require.Len(t, closed, 1, "окно закрыто по простою")
	require.Equal(t, 0, a.Stats().OpenWindows)

	addLive(a, trade("BTC-USDT", "150", "5", baseTime.Add(20*time.Second)))

	stats := a.Stats()
	require.Equal(t, int64(1), stats.Late)
	require.Equal(t, 0, stats.OpenWindows, "закрытое окно не открывается заново")
}

// TestMultipleTimeframesFromOneStream — одна сделка правит два окна разной
// длины, и закрываются они независимо.
func TestMultipleTimeframesFromOneStream(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t, domain.TF1s, domain.TF1m)

	addLive(a, trade("BTC-USDT", "100", "1", baseTime.Add(100*time.Millisecond)))
	addLive(a, trade("BTC-USDT", "110", "1", baseTime.Add(200*time.Millisecond)))

	require.Equal(t, 2, a.Stats().OpenWindows, "одно окно 1s и одно 1m")

	// Сделка следующей секунды: открывается второе секундное окно,
	// минутное остаётся тем же.
	addLive(a, trade("BTC-USDT", "120", "1", baseTime.Add(1200*time.Millisecond)))
	require.Equal(t, 3, a.Stats().OpenWindows)

	// Watermark 10:15:01.2 ещё не дошёл до дедлайна окна 10:15:00
	// (10:15:01 + 2s), закрывать нечего.
	require.Empty(t, a.Expired(baseTime))

	// Уводим время биржи вперёд: закрывается только секундное окно 10:15:00.
	addLive(a, trade("BTC-USDT", "130", "1", baseTime.Add(3500*time.Millisecond)))
	closed := a.Expired(baseTime)
	require.Len(t, closed, 1)
	require.Equal(t, domain.TF1s, closed[0].Timeframe)
	require.Equal(t, "100", closed[0].Open.String())
	require.Equal(t, "110", closed[0].Close.String())

	// Минутное окно всё это время набирало сделки всех секунд.
	closed = a.Expired(baseTime.Add(10 * time.Minute))
	minute := candleByTimeframe(t, closed, domain.TF1m)
	require.Equal(t, "100", minute.Open.String())
	require.Equal(t, "130", minute.High.String())
	require.Equal(t, "100", minute.Low.String())
	require.Equal(t, "130", minute.Close.String())
	require.Equal(t, int64(4), minute.TradeCount, "минутное окно видело все четыре сделки")
}

func candleByTimeframe(t *testing.T, candles []domain.Candle, tf domain.Timeframe) domain.Candle {
	t.Helper()

	for _, c := range candles {
		if c.Timeframe == tf {
			return c
		}
	}

	t.Fatalf("свеча с таймфреймом %s не найдена среди %d", tf, len(candles))
	return domain.Candle{}
}

// TestEmptyIntervalProducesNoCandle фиксирует решение из плана: пропуски
// в ряду допустимы, пустые окна не создаются.
func TestEmptyIntervalProducesNoCandle(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t, domain.TF1s)

	addLive(a, trade("BTC-USDC", "100", "1", baseTime.Add(500*time.Millisecond)))

	// Проходит пятнадцать секунд без единой сделки.
	closed := a.Expired(baseTime.Add(15 * time.Second))

	require.Len(t, closed, 1, "закрылось только окно с реальной сделкой")
	require.True(t, closed[0].OpenTime.Equal(baseTime))
	require.Equal(t, 0, a.Stats().OpenWindows)
}

// TestSymbolsAreIndependent проверяет, что watermark ведётся по инструменту:
// активность по BTC-USDT не должна закрывать окна редкого BTC-USDC.
func TestSymbolsAreIndependent(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t)

	addLive(a, trade("BTC-USDC", "100", "1", baseTime.Add(time.Second)))
	addLive(a, trade("BTC-USDT", "200", "1", baseTime.Add(time.Second)))

	// BTC-USDT уходит далеко вперёд по времени биржи.
	addLive(a, trade("BTC-USDT", "210", "1", baseTime.Add(5*time.Minute)))

	// Системные часы стоят на 10:15:30 — порог простоя не достигнут ни для
	// одного окна, поэтому закрывается ровно то, что закрыл watermark.
	closed := a.Expired(baseTime.Add(30 * time.Second))
	require.Len(t, closed, 1)
	require.Equal(t, domain.Symbol("BTC-USDT"), closed[0].Symbol)

	// Живы окно BTC-USDC 10:15 и новое окно BTC-USDT 10:20: у первого
	// собственный watermark не двигался, второе только что открылось.
	require.Equal(t, 2, a.Stats().OpenWindows)

	remaining := a.Flush()
	require.Len(t, remaining, 2)
	require.Equal(t, domain.Symbol("BTC-USDC"), remaining[0].Symbol)
	require.True(t, remaining[0].OpenTime.Equal(baseTime))
	require.Equal(t, domain.Symbol("BTC-USDT"), remaining[1].Symbol)
	require.True(t, remaining[1].OpenTime.Equal(baseTime.Add(5*time.Minute)))
}

func TestFlushClosesPartialWindows(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t, domain.TF1s, domain.TF1m)

	addLive(a, trade("BTC-USDT", "100", "1", baseTime.Add(time.Second)))
	addLive(a, trade("ETH-USDT", "50", "2", baseTime.Add(time.Second)))
	require.Equal(t, 4, a.Stats().OpenWindows)

	closed := a.Flush()

	require.Len(t, closed, 4, "при остановке пишутся все открытые окна")
	require.Equal(t, 0, a.Stats().OpenWindows)
	require.Empty(t, a.Flush(), "повторный вызов ничего не находит")
}

// TestClosedCandlesAreSorted проверяет детерминированный порядок выдачи:
// обход мапы окон случаен, а потребителю нужен стабильный порядок.
func TestClosedCandlesAreSorted(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t, domain.TF1s, domain.TF1m)

	addLive(a, trade("ETH-USDT", "50", "1", baseTime.Add(2*time.Second)))
	addLive(a, trade("BTC-USDT", "100", "1", baseTime.Add(time.Second)))
	addLive(a, trade("BTC-USDC", "70", "1", baseTime.Add(time.Second)))

	closed := a.Flush()
	require.Len(t, closed, 6)

	for i := 1; i < len(closed); i++ {
		prev, cur := closed[i-1], closed[i]
		switch {
		case !prev.OpenTime.Equal(cur.OpenTime):
			require.True(t, prev.OpenTime.Before(cur.OpenTime))
		case prev.Symbol != cur.Symbol:
			require.Less(t, string(prev.Symbol), string(cur.Symbol))
		default:
			require.LessOrEqual(t, string(prev.Timeframe), string(cur.Timeframe))
		}
	}
}

func TestStats(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t, domain.TF1s, domain.TF1m)

	addLive(a, trade("BTC-USDT", "100", "1", baseTime.Add(time.Second)))
	addLive(a, trade("BTC-USDT", "110", "1", baseTime.Add(2*time.Second)))
	a.Expired(baseTime.Add(10 * time.Minute))

	stats := a.Stats()
	require.Equal(t, int64(2), stats.Trades)
	require.Equal(t, int64(0), stats.Late)
	require.Equal(t, int64(2), stats.Closed[domain.TF1s], "две секундные свечи")
	require.Equal(t, int64(1), stats.Closed[domain.TF1m], "одна минутная")
	require.Equal(t, 0, stats.OpenWindows)
}

// TestOpenWindowsAreBounded проверяет, что несколько окон на ключ не текут:
// тикер закрывает старые, и их число не растёт с длиной потока.
func TestOpenWindowsAreBounded(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t, domain.TF1s)

	for i := range 600 {
		at := baseTime.Add(time.Duration(i) * 100 * time.Millisecond)
		addLive(a, trade("BTC-USDT", "100", "1", at))
		a.Expired(at)
	}

	// Живыми остаются только окна внутри grace period, а не все 60 секунд.
	require.LessOrEqual(t, a.Stats().OpenWindows, 4)
	require.Equal(t, int64(0), a.Stats().Late)
}

// TestBacklogReplayKeepsWholeWindow воспроизводит разбор накопленного в
// топике: время событий далеко позади системных часов, а сделки одной минуты
// приходят двумя батчами с проверкой дедлайнов между ними.
//
// Без второго условия в проверке простоя окно закрывалось бы сразу после
// первого батча, и сделки второго терялись бы как опоздавшие — свеча вышла
// бы с заниженными объёмом и числом сделок.
func TestBacklogReplayKeepsWholeWindow(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t)

	// Часы сервиса на час впереди времени биржи — так выглядит разбор хвоста.
	wall := baseTime.Add(time.Hour)

	// Первый батч: половина минуты.
	a.Add(trade("BTC-USDT", "100", "1", baseTime.Add(10*time.Second)), wall)
	a.Add(trade("BTC-USDT", "110", "1", baseTime.Add(20*time.Second)), wall)

	require.Empty(t, a.Expired(wall), "окно не должно закрыться посреди разбора")

	// Второй батч приходит почти сразу — так и работает перечитывание.
	wall = wall.Add(50 * time.Millisecond)
	a.Add(trade("BTC-USDT", "90", "1", baseTime.Add(40*time.Second)), wall)
	a.Add(trade("BTC-USDT", "105", "1", baseTime.Add(50*time.Second)), wall)

	require.Equal(t, int64(0), a.Stats().Late, "сделки второго батча не опоздали")

	// Окно закрывает watermark, когда доходит очередь до следующей минуты.
	wall = wall.Add(50 * time.Millisecond)
	a.Add(trade("BTC-USDT", "120", "1", baseTime.Add(65*time.Second)), wall)

	closed := a.Expired(wall)
	require.Len(t, closed, 1)
	require.Equal(t, int64(4), closed[0].TradeCount, "в свече все четыре сделки минуты")
	require.Equal(t, "110", closed[0].High.String())
	require.Equal(t, "90", closed[0].Low.String())
	require.Equal(t, "4", closed[0].Volume.String())
}

// TestIdleCloseWaitsForWindowPeriod проверяет вторую половину условия простоя:
// окно нельзя закрыть раньше, чем истечёт его собственный интервал, иначе
// минутная свеча схлопнулась бы через несколько секунд после первой сделки.
func TestIdleCloseWaitsForWindowPeriod(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t)

	addLive(a, trade("BTC-USDC", "100", "1", baseTime.Add(time.Second)))

	// Простоя уже больше grace+idle, но минута ещё не кончилась.
	require.Empty(t, a.Expired(baseTime.Add(30*time.Second)))
	require.Equal(t, 1, a.Stats().OpenWindows)
}
