-- Ticker symbols for the automation pipeline (id = yfinance symbol, name = company name)
CREATE TABLE IF NOT EXISTS tickers (
    id VARCHAR(30) PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

INSERT INTO tickers (id, name) VALUES
    ('AAPL', 'Apple Inc.'),
    ('IBM', 'International Business Machines'),
    ('MSFT', 'Microsoft Corporation'),
    ('GOOG', 'Alphabet Inc. (Google)'),
    ('AMZN', 'Amazon.com, Inc.'),
    ('NFLX', 'Netflix, Inc.'),
    ('TSLA', 'Tesla, Inc.'),
    ('META', 'Meta Platforms, Inc. (Facebook)'),
    ('NVDA', 'NVIDIA Corporation'),
    ('ORCL', 'Oracle Corporation')
ON CONFLICT (id) DO NOTHING;
