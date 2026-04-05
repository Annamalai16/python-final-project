CREATE TABLE IF NOT EXISTS enriched_daily (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(30) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(12, 4) NOT NULL,
    high NUMERIC(12, 4) NOT NULL,
    low NUMERIC(12, 4) NOT NULL,
    close NUMERIC(12, 4) NOT NULL,
    volume BIGINT NOT NULL,
    lag_1 NUMERIC(12, 4),
    lag_2 NUMERIC(12, 4),
    lag_3 NUMERIC(12, 4),
    lag_5 NUMERIC(12, 4),
    lag_10 NUMERIC(12, 4),
    ma_5 NUMERIC(12, 4),
    ma_10 NUMERIC(12, 4),
    ma_20 NUMERIC(12, 4),
    rolling_std_5 NUMERIC(12, 6),
    rolling_std_10 NUMERIC(12, 6),
    daily_return NUMERIC(12, 6),
    return_5 NUMERIC(12, 6),
    return_10 NUMERIC(12, 6),
    return_20 NUMERIC(12, 6),
    ma_cross NUMERIC(12, 6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, date)
);

CREATE INDEX IF NOT EXISTS idx_enriched_daily_ticker_date ON enriched_daily(ticker, date);
CREATE INDEX IF NOT EXISTS idx_enriched_daily_date ON enriched_daily(date);
