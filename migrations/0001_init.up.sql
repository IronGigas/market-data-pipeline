-- Свечи OHLCV по инструменту и таймфрейму.
--
-- Первичный ключ (symbol, timeframe, open_time) — не техническая деталь,
-- а основа идемпотентности: агрегатор доставляет свечи с семантикой
-- at-least-once, и повторная запись той же свечи обязана быть безвредной.
CREATE TABLE IF NOT EXISTS candles (
    symbol      TEXT        NOT NULL,
    timeframe   TEXT        NOT NULL,
    open_time   TIMESTAMPTZ NOT NULL,
    close_time  TIMESTAMPTZ NOT NULL,

    -- NUMERIC, а не DOUBLE PRECISION: цены и объёмы приходят десятичными
    -- строками, и двоичная плавающая точка исказила бы последние знаки.
    -- 38 знаков всего, 12 после запятой — с запасом на любой инструмент.
    open        NUMERIC(38, 12) NOT NULL,
    high        NUMERIC(38, 12) NOT NULL,
    low         NUMERIC(38, 12) NOT NULL,
    close       NUMERIC(38, 12) NOT NULL,
    volume      NUMERIC(38, 12) NOT NULL,

    trade_count BIGINT      NOT NULL,

    -- Отметка последней записи: по ней видно, что свеча была перезаписана
    -- при повторной обработке батча.
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (symbol, timeframe, open_time)
);

-- Основной сценарий чтения — «последние N свечей по инструменту и
-- таймфрейму». Порядок DESC в индексе позволяет взять их без сортировки.
CREATE INDEX IF NOT EXISTS candles_symbol_tf_time_idx
    ON candles (symbol, timeframe, open_time DESC);
