package binance

import (
	"fmt"
	"strings"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

// symbolMap переводит доменные символы в тикеры Binance и обратно.
//
// Прямое направление тривиально: BTC-USDT -> btcusdt. Обратное вычислить
// нельзя: по строке BTCUSDT не видно, где кончается база и начинается
// котировка (USDT против USD + T). Поэтому обратный маппинг — не алгоритм,
// а таблица, построенная из списка подписок: сообщение по инструменту,
// на который мы не подписаны, всё равно нам не нужно.
type symbolMap struct {
	toTicker map[domain.Symbol]string
	toDomain map[string]domain.Symbol
}

// newSymbolMap строит таблицу по списку доменных символов.
func newSymbolMap(symbols []domain.Symbol) (*symbolMap, error) {
	m := &symbolMap{
		toTicker: make(map[domain.Symbol]string, len(symbols)),
		toDomain: make(map[string]domain.Symbol, len(symbols)),
	}

	for _, symbol := range symbols {
		ticker := ticker(symbol)
		if ticker == "" {
			return nil, fmt.Errorf("%w: %q maps to an empty ticker", domain.ErrInvalidSymbol, symbol)
		}
		// Два доменных символа не могут дать один тикер при корректном
		// списке подписок, но проверка дешёвая, а последствие коллизии —
		// молча потерянный инструмент.
		if existing, ok := m.toDomain[ticker]; ok {
			return nil, fmt.Errorf("%w: %q and %q map to the same ticker %q", domain.ErrInvalidSymbol, existing, symbol, ticker)
		}

		m.toTicker[symbol] = ticker
		m.toDomain[ticker] = symbol
	}

	return m, nil
}

// ticker переводит доменный символ в тикер биржи: BTC-USDT -> btcusdt.
func ticker(symbol domain.Symbol) string {
	return strings.ToLower(strings.ReplaceAll(string(symbol), "-", ""))
}

// stream возвращает имя стрима сделок для доменного символа.
func (m *symbolMap) stream(symbol domain.Symbol) string {
	return m.toTicker[symbol] + "@trade"
}

// streams возвращает имена стримов в порядке переданных символов.
func (m *symbolMap) streams(symbols []domain.Symbol) []string {
	names := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		names = append(names, m.stream(symbol))
	}
	return names
}

// symbol переводит тикер биржи в доменный символ. Регистр тикера в поле "s"
// сообщения — верхний, в имени стрима — нижний, поэтому сравнение идёт
// по нормализованной форме.
func (m *symbolMap) symbol(exchangeTicker string) (domain.Symbol, bool) {
	s, ok := m.toDomain[strings.ToLower(strings.TrimSpace(exchangeTicker))]
	return s, ok
}
