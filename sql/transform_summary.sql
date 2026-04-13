-- Truncate and repopulate the summary table
TRUNCATE TABLE stock_daily_summary;

INSERT INTO stock_daily_summary (
    ticker,
    trade_date,
    close_price,
    daily_change_pct,
    avg_7day_close,
    volume,
    volume_spike
)
SELECT
    ticker,
    trade_date,
    close_price,

    -- Daily % change vs previous day
    ROUND(
        (close_price - LAG(close_price) OVER (
            PARTITION BY ticker ORDER BY trade_date
        )) /
        NULLIF(LAG(close_price) OVER (
            PARTITION BY ticker ORDER BY trade_date
        ), 0) * 100
    , 2) AS daily_change_pct,

    -- 7-day moving average of close price
    ROUND(
        AVG(close_price) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )
    , 4) AS avg_7day_close,

    volume,

    -- Flag volume spikes: today's volume > 2x the 7-day avg volume
    CASE
        WHEN volume > 2 * AVG(volume) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) THEN TRUE
        ELSE FALSE
    END AS volume_spike

FROM raw_stock_prices
ORDER BY ticker, trade_date;