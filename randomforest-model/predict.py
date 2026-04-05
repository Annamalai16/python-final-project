import numpy as np
import pandas as pd
import joblib
import os
import logging
import tempfile
import boto3
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(SCRIPT_DIR, 'rf_model.joblib')

FEATURE_NAMES = [
    'lag_1_ratio', 'lag_2_ratio', 'lag_3_ratio', 'lag_5_ratio', 'lag_10_ratio',
    'ma_5_ratio', 'ma_10_ratio', 'ma_20_ratio',
    'std_5_ratio', 'std_10_ratio',
    'daily_return', 'return_5', 'return_10', 'return_20',
    'ma_cross',
]

_model = None

def load_model():
    global _model
    if _model is not None:
        return _model

    bucket = os.environ.get("S3_BUCKET_NAME")
    s3_key = os.environ.get("MODEL_S3_KEY", "models/rf_model.joblib")

    if bucket:
        logger.info("Loading model from s3://%s/%s", bucket, s3_key)
        s3 = boto3.client('s3')
        with tempfile.NamedTemporaryFile(suffix='.joblib', delete=False) as tmp:
            tmp_path = tmp.name
        try:
            s3.download_file(bucket, s3_key, tmp_path)
            _model = joblib.load(tmp_path)
            logger.info("Model loaded from S3 successfully")
        finally:
            os.unlink(tmp_path)
    else:
        if not os.path.exists(MODEL_PATH):
            raise FileNotFoundError(
                f"Model file not found at {MODEL_PATH} and S3_BUCKET_NAME is not set. "
                "Run train_model.py first to generate it."
            )
        _model = joblib.load(MODEL_PATH)
        logger.info("Loaded model from local path %s", MODEL_PATH)

    return _model


def _safe_get(history: list, neg_index: int, default: float) -> float:
    if len(history) >= abs(neg_index):
        return history[neg_index]
    return default


def _build_features(close_history: list) -> np.ndarray:
    current = close_history[-1]
    if current == 0:
        current = 1e-8

    lag_1 = _safe_get(close_history, -2, current)
    lag_2 = _safe_get(close_history, -3, current)
    lag_3 = _safe_get(close_history, -4, current)
    lag_5 = _safe_get(close_history, -6, current)
    lag_10 = _safe_get(close_history, -11, current)

    window_5 = close_history[-5:] if len(close_history) >= 5 else close_history
    window_10 = close_history[-10:] if len(close_history) >= 10 else close_history
    window_20 = close_history[-20:] if len(close_history) >= 20 else close_history

    ma_5 = np.mean(window_5)
    ma_10 = np.mean(window_10)
    ma_20 = np.mean(window_20)

    std_5 = float(np.std(window_5, ddof=1)) if len(window_5) >= 2 else 0.0
    std_10 = float(np.std(window_10, ddof=1)) if len(window_10) >= 2 else 0.0

    daily_ret = (current - lag_1) / lag_1 if lag_1 != 0 else 0.0

    close_5_ago = _safe_get(close_history, -6, close_history[0])
    close_10_ago = _safe_get(close_history, -11, close_history[0])
    close_20_ago = _safe_get(close_history, -21, close_history[0])

    return_5 = (current - close_5_ago) / close_5_ago if close_5_ago != 0 else 0.0
    return_10 = (current - close_10_ago) / close_10_ago if close_10_ago != 0 else 0.0
    return_20 = (current - close_20_ago) / close_20_ago if close_20_ago != 0 else 0.0

    ma_cross = ma_5 / ma_20 if ma_20 != 0 else 1.0

    return np.array([[
        lag_1 / current,
        lag_2 / current,
        lag_3 / current,
        lag_5 / current,
        lag_10 / current,
        ma_5 / current,
        ma_10 / current,
        ma_20 / current,
        std_5 / current,
        std_10 / current,
        daily_ret,
        return_5,
        return_10,
        return_20,
        ma_cross,
    ]])


def predict_ticker(close_history: list) -> list:
    model = load_model()
    features = _build_features(close_history)
    predicted_ratios = model.predict(features)[0]
    current_close = close_history[-1]
    return [round(current_close * ratio, 4) for ratio in predicted_ratios]


def _next_n_weekdays(from_date, n: int) -> list:
    dates = []
    current = from_date
    while len(dates) < n:
        current = current + timedelta(days=1)
        if current.weekday() < 5:
            dates.append(current)
    return dates


def predict_all_tickers(
    last_30_days_df: pd.DataFrame,
    fallback_historical: Optional[Dict[str, Any]] = None,
    fallback_current: Optional[Dict[str, Dict]] = None,
    run_date=None,
) -> Dict[str, List[Dict[str, Any]]]:
    load_model()

    today = run_date if run_date is not None else datetime.now().date()
    predictions_map: Dict[str, List[Dict[str, Any]]] = {}

    tickers = set()
    if not last_30_days_df.empty:
        tickers.update(last_30_days_df['ticker'].unique())
    if fallback_historical:
        tickers.update(fallback_historical.keys())

    if not tickers:
        logger.warning("No tickers available for prediction.")
        return {}

    min_history = 20

    for ticker in sorted(tickers):
        close_history: List[float] = []

        if not last_30_days_df.empty:
            ticker_df = last_30_days_df[last_30_days_df['ticker'] == ticker].sort_values('date')
            close_history = ticker_df['close'].astype(float).tolist()

        if len(close_history) < min_history and fallback_historical and ticker in fallback_historical:
            hist_df = fallback_historical[ticker]
            hist_closes = hist_df['close'].astype(float).tolist()
            if fallback_current and ticker in fallback_current:
                hist_closes.append(float(fallback_current[ticker]['close']))
            close_history = hist_closes
            logger.info(
                "Using yfinance historical data for %s (%d close prices)",
                ticker, len(close_history),
            )

        if not close_history:
            logger.warning("No close price data for %s, skipping.", ticker)
            continue

        predicted_closes = predict_ticker(close_history)

        forecast_dates = _next_n_weekdays(today, len(predicted_closes))
        preds = []
        for forecast_date, pred_close in zip(forecast_dates, predicted_closes):
            preds.append({
                "forecast_date": forecast_date.strftime("%Y-%m-%d"),
                "predicted_close": pred_close,
            })

        predictions_map[ticker] = preds
        last_close = close_history[-1]
        logger.info(
            "Predicted 7 days for %s (last_close=%.2f, day+1=%.2f, day+7=%.2f)",
            ticker, last_close, predicted_closes[0], predicted_closes[-1],
        )

    logger.info("Predictions generated for %d tickers.", len(predictions_map))
    return predictions_map
