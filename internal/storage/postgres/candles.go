package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/IronGigas/market-data-pipeline/internal/domain"
)

// upsertCandle — идемпотентная запись свечи.
//
// open намеренно не обновляется: цена открытия окна задана его первой
// сделкой и меняться не может. При повторной обработке батча остальные
// поля перезаписываются свежим расчётом, а open остаётся исходным.
const upsertCandle = `
INSERT INTO candles (
    symbol, timeframe, open_time, close_time,
    open, high, low, close, volume, trade_count
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (symbol, timeframe, open_time) DO UPDATE SET
    high        = EXCLUDED.high,
    low         = EXCLUDED.low,
    close       = EXCLUDED.close,
    volume      = EXCLUDED.volume,
    trade_count = EXCLUDED.trade_count,
    close_time  = EXCLUDED.close_time,
    updated_at  = now()`

// CandleStore пишет и читает свечи.
type CandleStore struct {
	db *DB
}

// NewCandleStore создаёт хранилище поверх существующего пула.
func NewCandleStore(db *DB) *CandleStore {
	return &CandleStore{db: db}
}

// Upsert записывает пачку свечей одним пакетом запросов.
//
// Пакет, а не отдельные запросы: свечи закрываются группами (на границе
// минуты созревают все инструменты сразу), и каждый отдельный round trip
// к базе стоил бы дороже самой записи.
//
// Пакет не обёрнут в транзакцию сознательно. Каждая запись идемпотентна
// сама по себе, а частично применённый пакет исправится следующей попыткой:
// повтор перезапишет уже записанные свечи теми же значениями.
func (s *CandleStore) Upsert(ctx context.Context, candles []domain.Candle) error {
	if len(candles) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, candle := range candles {
		batch.Queue(upsertCandle,
			candle.Symbol.String(),
			candle.Timeframe.String(),
			candle.OpenTime,
			candle.CloseTime,
			// decimal уходит строкой: PostgreSQL приводит её к NUMERIC без
			// потери знаков, тогда как любой промежуточный float64 исказил
			// бы последние разряды.
			candle.Open.String(),
			candle.High.String(),
			candle.Low.String(),
			candle.Close.String(),
			candle.Volume.String(),
			candle.TradeCount,
		)
	}

	results := s.db.pool.SendBatch(ctx, batch)
	for i, candle := range candles {
		if _, err := results.Exec(); err != nil {
			results.Close()
			return fmt.Errorf("postgres: upsert candle %d (%s %s %s): %w",
				i, candle.Symbol, candle.Timeframe, candle.OpenTime.Format("2006-01-02T15:04:05Z"), err)
		}
	}

	if err := results.Close(); err != nil {
		return fmt.Errorf("postgres: close batch: %w", err)
	}
	return nil
}
