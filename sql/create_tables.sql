-- Raw layer: exactly what the API returns
CREATE TABLE IF NOT EXISTS raw_stock_prices (
    id              SERIAL PRIMARY KEY,
    ticker          VARCHAR(10)    NOT NULL,
    trade_date      DATE           NOT NULL,
    open_price      NUMERIC(10,4)  NOT NULL,
    high_price      NUMERIC(10,4)  NOT NULL,
    low_price       NUMERIC(10,4)  NOT NULL,
    close_price     NUMERIC(10,4)  NOT NULL,
    volume          BIGINT         NOT NULL,
    ingested_at     TIMESTAMP      DEFAULT NOW(),
    UNIQUE (ticker, trade_date)
);

-- Transformed layer: analytical summaries
CREATE TABLE IF NOT EXISTS stock_daily_summary (
    id                  SERIAL PRIMARY KEY,
    ticker              VARCHAR(10)   NOT NULL,
    trade_date          DATE          NOT NULL,
    close_price         NUMERIC(10,4) NOT NULL,
    daily_change_pct    NUMERIC(6,2),
    avg_7day_close      NUMERIC(10,4),
    volume              BIGINT,
    volume_spike        BOOLEAN       DEFAULT FALSE,
    updated_at          TIMESTAMP     DEFAULT NOW(),
    UNIQUE (ticker, trade_date)
);