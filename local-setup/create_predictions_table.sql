CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(30) NOT NULL,
    forecast_date DATE NOT NULL,
    predicted_close NUMERIC(12, 4) NOT NULL,
    prediction_date DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_predictions_ticker_date ON predictions(ticker, forecast_date);
CREATE INDEX IF NOT EXISTS idx_predictions_prediction_date ON predictions(prediction_date);
CREATE INDEX IF NOT EXISTS idx_predictions_created_at ON predictions(created_at);
