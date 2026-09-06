package aggregate

import (
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

// Значения по умолчанию для Config.
const (
	// defaultGrace — сколько ждать опоздавшие сделки после границы окна.
	// Две секунды заметно больше типичной задержки доставки от биржи и
	// заметно меньше времени, за которое свеча перестанет быть свежей.
	defaultGrace = 2 * time.Second

	// defaultIdleTimeout — добавка к дедлайну для окон, по которым поток
	// замолчал. Watermark двигают только новые сделки, а по редкому
	// инструменту следующая может прийти через минуты — без этой добавки
	// свеча висела бы открытой до неё.
	defaultIdleTimeout = 3 * time.Second
)

// Config — параметры агрегатора.
type Config struct {
	// Timeframes — таймфреймы, которые считаются параллельно из одного
	// потока сделок.
	Timeframes []domain.Timeframe

	Grace       time.Duration
	IdleTimeout time.Duration

	Logger *slog.Logger
}

// Stats — счётчики агрегатора на момент вызова.
type Stats struct {
	Trades      int64                      // сделки, принятые в окна
	Late        int64                      // отброшено как опоздавшие
	Closed      map[domain.Timeframe]int64 // закрыто свечей по таймфреймам
	OpenWindows int
}

// windowID адресует конкретное окно среди открытых.
//
// Время начала окна хранится числом, а не time.Time: сравнение time.Time
// оператором == учитывает монотонные часы и указатель на зону, и ключ мапы
// на нём — источник трудноуловимых ошибок. UnixNano однозначен.
type windowID struct {
	Symbol    domain.Symbol
	Timeframe domain.Timeframe
	OpenUnix  int64
}

func newWindowID(key WindowKey, openTime time.Time) windowID {
	return windowID{
		Symbol:    key.Symbol,
		Timeframe: key.Timeframe,
		OpenUnix:  openTime.UnixNano(),
	}
}

// Aggregator хранит открытые окна и закрывает их по дедлайнам.
//
// На один ключ может быть открыто несколько окон одновременно. Это ключевое
// решение: приход сделки следующего интервала не закрывает предыдущее окно,
// а открывает рядом новое. Иначе grace period не работал бы вовсе — любая
// сделка, способная закрыть окно по watermark, лежит за его границей, то
// есть принадлежит следующему окну и закрывала бы текущее немедленно.
// Разделение ролей получается чистым: Add только наполняет окна, Expired
// только закрывает.
//
// Состояние защищено одним мьютексом: его берут по очереди обработка батча
// из Kafka и тикер проверки дедлайнов. На трёх инструментах и двух
// таймфреймах конкуренция за него пренебрежимо мала. Шардирование по
// символу — очевидный путь масштабирования, но в MVP это лишняя сложность.
type Aggregator struct {
	mu sync.Mutex

	timeframes  []domain.Timeframe
	grace       time.Duration
	idleTimeout time.Duration
	log         *slog.Logger

	// windows — открытые окна. Их число ограничено сверху: окно живёт не
	// дольше своего дедлайна, а тикер ходит чаще, чем grace period.
	windows map[windowID]*Window

	// watermark — максимальный EventTime, виденный по инструменту.
	// Это ответ на вопрос «докуда по времени биржи мы точно всё получили»,
	// и именно он, а не системные часы, закрывает окна в нормальном режиме.
	watermark map[domain.Symbol]time.Time

	// lastClosed — OpenTime самого позднего закрытого окна по ключу.
	//
	// Нужен, чтобы опоздавшая сделка не воскресила уже закрытое окно: после
	// закрытия окна в мапе нет и сравнивать сделку не с чем. Без этого она
	// открыла бы окно заново и породила вторую свечу с тем же ключом, но с
	// испорченной ценой открытия.
	lastClosed map[WindowKey]time.Time

	trades int64
	late   int64
	closed map[domain.Timeframe]int64
}

// New собирает агрегатор. Зависимости приходят снаружи: пакет не читает
// окружение и не создаёт логгер.
func New(cfg Config) (*Aggregator, error) {
	if len(cfg.Timeframes) == 0 {
		return nil, errors.New("aggregate: no timeframes configured")
	}
	if cfg.Logger == nil {
		return nil, errors.New("aggregate: logger is required")
	}

	grace := cfg.Grace
	if grace <= 0 {
		grace = defaultGrace
	}
	idleTimeout := cfg.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = defaultIdleTimeout
	}

	timeframes := make([]domain.Timeframe, len(cfg.Timeframes))
	copy(timeframes, cfg.Timeframes)

	return &Aggregator{
		timeframes:  timeframes,
		grace:       grace,
		idleTimeout: idleTimeout,
		log:         cfg.Logger,
		windows:     make(map[windowID]*Window),
		watermark:   make(map[domain.Symbol]time.Time),
		lastClosed:  make(map[WindowKey]time.Time),
		closed:      make(map[domain.Timeframe]int64),
	}, nil
}

// Add разносит сделку по окнам всех таймфреймов.
//
// now — текущее время по часам сервиса; оно запоминается в окне и позже
// используется для отсчёта простоя. Передаётся параметром, а не берётся
// внутри: иначе логику закрытия окон нельзя было бы проверить тестами,
// не дожидаясь реальных секунд.
//
// Ничего не закрывает и не возвращает: закрытием занимается только Expired.
// Вызывающий обязан дёргать Expired регулярно, иначе свечи не появятся.
func (a *Aggregator) Add(trade domain.Trade, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, tf := range a.timeframes {
		a.addToTimeframe(trade, tf, now)
	}

	a.trades++

	// Watermark двигается один раз на сделку, независимо от числа
	// таймфреймов: это свойство потока по инструменту, а не окна.
	if current, ok := a.watermark[trade.Symbol]; !ok || trade.EventTime.After(current) {
		a.watermark[trade.Symbol] = trade.EventTime
	}
}

// addToTimeframe кладёт сделку в её окно, открывая его при необходимости.
func (a *Aggregator) addToTimeframe(trade domain.Trade, tf domain.Timeframe, now time.Time) {
	openTime := tf.Truncate(trade.EventTime)
	key := WindowKey{Symbol: trade.Symbol, Timeframe: tf}

	// Окно этой сделки уже закрыто — пересчитывать закрытую свечу в MVP
	// не умеем. Пока окно открыто, сделка примется, даже если пришла с
	// опозданием: ровно ради этого и существует grace period.
	if last, ok := a.lastClosed[key]; ok && !openTime.After(last) {
		a.late++
		a.log.Debug("late trade dropped",
			slog.String("symbol", trade.Symbol.String()),
			slog.String("timeframe", tf.String()),
			slog.Int64("trade_id", trade.TradeID),
			slog.Time("event_time", trade.EventTime),
			slog.Time("window_open_time", openTime))
		return
	}

	id := newWindowID(key, openTime)
	if window, ok := a.windows[id]; ok {
		window.apply(trade, now)
		return
	}

	a.windows[id] = newWindow(key, openTime, trade, now)
}

// Expired закрывает окна, у которых наступил дедлайн, и возвращает свечи.
//
// Вызывается тикером независимо от прихода сделок: по редкому инструменту
// новых сделок может не быть минутами, а окно закрыть надо.
func (a *Aggregator) Expired(now time.Time) []domain.Candle {
	now = now.UTC()

	a.mu.Lock()
	defer a.mu.Unlock()

	var closed []domain.Candle
	for id, window := range a.windows {
		if !a.isExpired(window, now) {
			continue
		}
		closed = append(closed, a.closeWindow(id, window))
	}

	return sortCandles(closed)
}

// isExpired проверяет два независимых условия закрытия.
func (a *Aggregator) isExpired(window *Window, now time.Time) bool {
	deadline := window.deadline(a.grace)

	// Основное условие — по времени биржи: сделки этого инструмента уже
	// ушли за дедлайн окна, значит в него больше ничего не придёт.
	if watermark, ok := a.watermark[window.Key.Symbol]; ok && !watermark.Before(deadline) {
		return true
	}

	// Страховка на случай, если поток по инструменту иссяк и watermark стоит
	// на месте. Без неё последняя свеча замолчавшего инструмента висела бы
	// открытой бесконечно.
	//
	// Условий два, и оба обязательны.
	//
	// Первое — интервал окна истёк по системным часам. Без него минутное
	// окно закрывалось бы через несколько секунд после первой сделки,
	// не дождавшись своей минуты.
	//
	// Второе — в окно давно ничего не клали. Без него страховка ломается при
	// разборе накопленного в топике: там системные часы уходят вперёд от
	// времени биржи на часы, первое условие истинно сразу, и окно
	// закрывалось бы прямо после создания, а сделки той же минуты из
	// следующего батча терялись бы как опоздавшие.
	//
	// В живой работе обе шкалы идут рядом, и условия выполняются почти
	// одновременно — поведение то же, что и без второго.
	return !now.Before(deadline.Add(a.idleTimeout)) &&
		!now.Before(window.UpdatedAt.Add(a.grace+a.idleTimeout))
}

// Flush закрывает все открытые окна и возвращает их свечи.
//
// Используется при остановке сервиса. Свечи будут неполными: окно
// закрывается раньше своей границы. Это осознанный размен — потерять
// частичную свечу хуже, чем записать её. Ограничение задокументировано
// в README.
func (a *Aggregator) Flush() []domain.Candle {
	a.mu.Lock()
	defer a.mu.Unlock()

	closed := make([]domain.Candle, 0, len(a.windows))
	for id, window := range a.windows {
		closed = append(closed, a.closeWindow(id, window))
	}

	return sortCandles(closed)
}

// closeWindow снимает окно с учёта и превращает его в свечу.
// Вызывается с уже взятым мьютексом.
func (a *Aggregator) closeWindow(id windowID, window *Window) domain.Candle {
	delete(a.windows, id)
	a.closed[window.Key.Timeframe]++

	// Запоминается самое позднее закрытое окно: окна закрываются в порядке
	// дедлайнов, но полагаться на порядок обхода мапы нельзя.
	if last, ok := a.lastClosed[window.Key]; !ok || window.OpenTime.After(last) {
		a.lastClosed[window.Key] = window.OpenTime
	}

	return window.Candle()
}

// Stats возвращает снимок счётчиков.
func (a *Aggregator) Stats() Stats {
	a.mu.Lock()
	defer a.mu.Unlock()

	closed := make(map[domain.Timeframe]int64, len(a.closed))
	for tf, n := range a.closed {
		closed[tf] = n
	}

	return Stats{
		Trades:      a.trades,
		Late:        a.late,
		Closed:      closed,
		OpenWindows: len(a.windows),
	}
}

// sortCandles упорядочивает свечи детерминированно: обход мапы окон даёт
// случайный порядок, а потребителю нужен стабильный — и в логах, и в
// тестах, и при записи в базу.
func sortCandles(candles []domain.Candle) []domain.Candle {
	sort.Slice(candles, func(i, j int) bool {
		if !candles[i].OpenTime.Equal(candles[j].OpenTime) {
			return candles[i].OpenTime.Before(candles[j].OpenTime)
		}
		if candles[i].Symbol != candles[j].Symbol {
			return candles[i].Symbol < candles[j].Symbol
		}
		return candles[i].Timeframe < candles[j].Timeframe
	})
	return candles
}
