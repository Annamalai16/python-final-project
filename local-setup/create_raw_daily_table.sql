CREATE TABLE IF NOT EXISTS raw_daily (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(30) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(12, 4) NOT NULL,
    high NUMERIC(12, 4) NOT NULL,
    low NUMERIC(12, 4) NOT NULL,
    close NUMERIC(12, 4) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, date)
);

CREATE INDEX IF NOT EXISTS idx_raw_daily_ticker_date ON raw_daily(ticker, date);
CREATE INDEX IF NOT EXISTS idx_raw_daily_date ON raw_daily(date);
