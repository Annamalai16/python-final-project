import yfinance as yf
import psycopg2
import pandas as pd
from typing import Dict, Optional, Any, Tuple, List
from datetime import datetime, timedelta
import os
import sys
import logging
from dotenv import load_dotenv

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_SCRIPT_DIR, 'randomforest-model'))

from predict import predict_all_tickers
from features import (
    lag_1,
    lag_2,
    lag_3,
    lag_5,
    lag_10,
    ma_5,
    ma_10,
    ma_20,
    rolling_std_5,
    rolling_std_10,
    daily_return,
    return_5,
    return_10,
    return_20,
    ma_cross,
)

try:
    _ENV_DIR = _SCRIPT_DIR
    load_dotenv(os.path.join(_ENV_DIR, ".env"))
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)


def _normalize_database_url(url: str) -> str:
    if not url:
        return url
    lower = url.lower()
    if not any(h in lower for h in ("supabase.co", "pooler.supabase.com")):
        return url
    if "sslmode=" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}sslmode=require"


def pgsql_from_environment() -> "PgSql":
    dsn = os.environ.get("DATABASE_URL")
    if dsn:
        dsn = dsn.strip().strip('"').strip("'")
        dsn = _normalize_database_url(dsn)
        return PgSql(dsn=dsn)
    return PgSql(
        host=os.environ.get("PGHOST", "localhost"),
        username=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", ""),
        db=os.environ.get("PGDATABASE", "postgres"),
        port=int(os.environ.get("PGPORT", "5432")),
    )


class PgSql:
    def __init__(
        self,
        dsn: Optional[str] = None,
        host: str = "localhost",
        username: str = "postgres",
        password: str = "",
        db: str = "postgres",
        port: int = 5432,
    ):
        self._dsn = dsn
        self.host = host
        self.username = username
        self.password = password
        self.db = db
        self.port = port
        self._conn = None
        self._cursor = None

    def connect(self) -> None:
        if self._conn is not None:
            logger.warning("Already connected.")
            return
        logger.info("Connecting to PostgreSQL database...")
        if self._dsn:
            self._conn = psycopg2.connect(self._dsn)
        else:
            if not self.password:
                raise ValueError(
                    "Set DATABASE_URL, or PGPASSWORD with PGHOST/PGUSER/PGDATABASE for local Postgres."
                )
            self._conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.db,
                user=self.username,
                password=self.password,
            )
        self._cursor = self._conn.cursor()
        logger.info("Connected to PostgreSQL.")

    def close(self) -> None:
        if self._cursor:
            self._cursor.close()
            self._cursor = None
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Database connection closed.")

    def execute_query(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> None:
        if self._cursor is None:
            raise RuntimeError("Not connected. Call connect() first.")
        self._cursor.execute(query, params)

    def fetch_all(self, query: str, params: Optional[Tuple[Any, ...]] = None) -> Tuple[List[Any], List[str]]:
        if self._cursor is None:
            raise RuntimeError("Not connected. Call connect() first.")
        self._cursor.execute(query, params)
        rows = self._cursor.fetchall()
        colnames = [d[0] for d in self._cursor.description] if self._cursor.description else []
        return rows, colnames

    def commit(self) -> None:
        if self._conn:
            self._conn.commit()

    def rollback(self) -> None:
        if self._conn:
            self._conn.rollback()


def fetchTickerList(pgsql: PgSql) -> Dict[str, str]:
    rows, _ = pgsql.fetch_all("SELECT id, name FROM tickers ORDER BY id", None)
    return {row[0]: row[1] for row in rows}


def PullTickerData(ticker_map: Dict[str, str], run_date) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, pd.DataFrame]]:
    actual_today = datetime.now().date()
    days_diff = (actual_today - run_date).days
    fetch_period_days = days_diff + 60

    logger.info(
        f"Pulling data for run_date={run_date} "
        f"(fetching {fetch_period_days} days from yFinance to cover 60 days of history)..."
    )
    current_day_data = {}
    historical_data = {}

    for ticker, company_name in ticker_map.items():
        try:
            logger.info(f"Fetching data for {ticker} ({company_name})...")

            data = yf.download(ticker, period=f"{fetch_period_days}d", interval="1d")

            if data.empty:
                logger.warning(f"No data returned for {ticker}")
                continue

            if isinstance(data.columns, pd.MultiIndex):
                data = data.copy()
                data.columns = [str(c).lower() for c in data.columns.get_level_values(0)]
            else:
                data = data.copy()
                data.columns = [c.lower() for c in data.columns]

            data = data[data.index.date <= run_date]

            if data.empty or len(data) < 31:
                logger.warning(f"Insufficient data for {ticker} up to {run_date} (need at least 31 days)")
                continue

            latest = data.iloc[-1]
            date_str = data.index[-1].strftime("%Y-%m-%d")

            if date_str != run_date.strftime("%Y-%m-%d"):
                logger.info(
                    f"run_date={run_date} has no trading data for {ticker} — "
                    f"latest available trading day is {date_str}. "
                    f"run_date may be a weekend or market holiday. Skipping this ticker."
                )
                continue

            current_day_data[ticker] = {
                "date": date_str,
                "open": float(latest["open"].iloc[0] if hasattr(latest["open"], "iloc") else latest["open"]),
                "high": float(latest["high"].iloc[0] if hasattr(latest["high"], "iloc") else latest["high"]),
                "low": float(latest["low"].iloc[0] if hasattr(latest["low"], "iloc") else latest["low"]),
                "close": float(latest["close"].iloc[0] if hasattr(latest["close"], "iloc") else latest["close"]),
                "volume": int(latest["volume"].iloc[0] if hasattr(latest["volume"], "iloc") else latest["volume"]),
            }

            hist = data.iloc[-31:-1][["open", "high", "low", "close", "volume"]].copy()
            historical_data[ticker] = hist

            logger.info(f"Fetched {ticker} - Date: {date_str}, Close: ${current_day_data[ticker]['close']:.2f}")

        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {str(e)}")
            continue

    logger.info(f"Data pull completed. Current day: {len(current_day_data)} tickers, historical: {len(historical_data)} tickers.")
    return current_day_data, historical_data


def InsertIntoRawDailyTable(pgsql: PgSql, ticker_data: Dict[str, Dict[str, float]]) -> None:
    if not ticker_data:
        logger.warning("No data to insert into database.")
        return

    insert_query = """
        INSERT INTO raw_daily (ticker, date, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, date)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            created_at = CURRENT_TIMESTAMP;
    """

    try:
        inserted_count = 0
        for ticker, data in ticker_data.items():
            try:
                pgsql.execute_query(
                    insert_query,
                    (
                        ticker,
                        data['date'],
                        data['open'],
                        data['high'],
                        data['low'],
                        data['close'],
                        data['volume']
                    )
                )
                inserted_count += 1
                logger.info(f"Inserted/Updated data for {ticker} on {data['date']}")
            except Exception as e:
                logger.error(f"Error inserting data for {ticker}: {str(e)}")
                continue

        pgsql.commit()
        logger.info(f"Successfully inserted/updated {inserted_count} records into raw_daily table.")

    except psycopg2.Error as e:
        logger.error(f"Database error: {str(e)}")
        pgsql.rollback()
        raise


def EnrichAndExtractFeatures(
    current_day_data: Dict[str, Dict[str, Any]],
    historical_data: Dict[str, pd.DataFrame],
) -> Dict[str, Dict[str, Any]]:
    logger.info("Enriching data and extracting features...")
    enriched = {}

    for ticker in current_day_data:
        if ticker not in historical_data:
            logger.warning(f"No historical data for {ticker}, skipping enrichment.")
            continue

        hist = historical_data[ticker]
        curr = current_day_data[ticker]

        curr_row = pd.DataFrame(
            [
                {
                    "open": curr["open"],
                    "high": curr["high"],
                    "low": curr["low"],
                    "close": curr["close"],
                    "volume": curr["volume"],
                }
            ],
            index=[pd.Timestamp(curr["date"])],
        )

        combined = pd.concat([hist, curr_row])
        combined = combined.astype(float, errors="ignore")

        combined = combined.ffill().bfill()

        close_series = combined["close"]

        enriched[ticker] = {
            "date": curr["date"],
            "open": float(combined["open"].iloc[-1]),
            "high": float(combined["high"].iloc[-1]),
            "low": float(combined["low"].iloc[-1]),
            "close": float(combined["close"].iloc[-1]),
            "volume": int(combined["volume"].iloc[-1]),
            "lag_1": lag_1(close_series),
            "lag_2": lag_2(close_series),
            "lag_3": lag_3(close_series),
            "lag_5": lag_5(close_series),
            "lag_10": lag_10(close_series),
            "ma_5": ma_5(close_series),
            "ma_10": ma_10(close_series),
            "ma_20": ma_20(close_series),
            "rolling_std_5": rolling_std_5(close_series),
            "rolling_std_10": rolling_std_10(close_series),
            "daily_return": daily_return(close_series),
            "return_5": return_5(close_series),
            "return_10": return_10(close_series),
            "return_20": return_20(close_series),
            "ma_cross": ma_cross(close_series),
        }
        logger.info(f"Enriched and featurized {ticker} for {curr['date']}")

    logger.info(f"Enrichment completed for {len(enriched)} tickers.")
    return enriched


def InsertIntoEnrichedDailyTable(
    pgsql: PgSql, enriched_data: Dict[str, Dict[str, Any]]
) -> None:
    if not enriched_data:
        logger.warning("No enriched data to insert.")
        return

    insert_query = """
        INSERT INTO enriched_daily (
            ticker, date, open, high, low, close, volume,
            lag_1, lag_2, lag_3, lag_5, lag_10,
            ma_5, ma_10, ma_20,
            rolling_std_5, rolling_std_10,
            daily_return, return_5, return_10, return_20,
            ma_cross
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, date)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            lag_1 = EXCLUDED.lag_1,
            lag_2 = EXCLUDED.lag_2,
            lag_3 = EXCLUDED.lag_3,
            lag_5 = EXCLUDED.lag_5,
            lag_10 = EXCLUDED.lag_10,
            ma_5 = EXCLUDED.ma_5,
            ma_10 = EXCLUDED.ma_10,
            ma_20 = EXCLUDED.ma_20,
            rolling_std_5 = EXCLUDED.rolling_std_5,
            rolling_std_10 = EXCLUDED.rolling_std_10,
            daily_return = EXCLUDED.daily_return,
            return_5 = EXCLUDED.return_5,
            return_10 = EXCLUDED.return_10,
            return_20 = EXCLUDED.return_20,
            ma_cross = EXCLUDED.ma_cross,
            created_at = CURRENT_TIMESTAMP;
    """

    try:
        inserted_count = 0
        for ticker, data in enriched_data.items():
            try:
                pgsql.execute_query(
                    insert_query,
                    (
                        ticker,
                        data["date"],
                        data["open"],
                        data["high"],
                        data["low"],
                        data["close"],
                        data["volume"],
                        data["lag_1"],
                        data["lag_2"],
                        data["lag_3"],
                        data["lag_5"],
                        data["lag_10"],
                        data["ma_5"],
                        data["ma_10"],
                        data["ma_20"],
                        data["rolling_std_5"],
                        data["rolling_std_10"],
                        data["daily_return"],
                        data["return_5"],
                        data["return_10"],
                        data["return_20"],
                        data["ma_cross"],
                    ),
                )
                inserted_count += 1
                logger.info(f"Inserted/Updated enriched data for {ticker} on {data['date']}")
            except Exception as e:
                logger.error(f"Error inserting enriched data for {ticker}: {str(e)}")
                continue

        pgsql.commit()
        logger.info(f"Successfully inserted/updated {inserted_count} records into enriched_daily table.")

    except psycopg2.Error as e:
        logger.error(f"Database error: {str(e)}")
        pgsql.rollback()
        raise


def PullLast30DaysFromPostgres(pgsql: PgSql, ticker_map: Dict[str, str], run_date) -> pd.DataFrame:
    logger.info(f"Pulling last 30 days enriched data from PostgreSQL (up to {run_date})...")
    tickers = tuple(ticker_map.keys())
    query = """
        SELECT ticker, date, open, high, low, close, volume,
               lag_1, lag_2, lag_3, lag_5, lag_10,
               ma_5, ma_10, ma_20,
               rolling_std_5, rolling_std_10,
               daily_return, return_5, return_10, return_20,
               ma_cross
        FROM enriched_daily
        WHERE date >= (%s - INTERVAL '30 days')
          AND date <= %s
          AND ticker IN %s
        ORDER BY ticker, date
    """
    rows, colnames = pgsql.fetch_all(query, (run_date, run_date, tickers))
    if not rows:
        logger.warning(f"No enriched data found for last 30 days up to {run_date}.")
        return pd.DataFrame(columns=colnames if colnames else None)
    df = pd.DataFrame(rows, columns=colnames)
    logger.info(f"Pulled {len(df)} rows for last 30 days ({df['ticker'].nunique()} tickers).")
    return df


def predictClosingPrice(
    last_30_days_df: pd.DataFrame,
    historical_data: Optional[Dict[str, pd.DataFrame]] = None,
    current_day_data: Optional[Dict[str, Dict[str, Any]]] = None,
    run_date=None,
) -> Dict[str, List[Dict[str, Any]]]:
    logger.info("Predicting next 7 days closing prices using Random Forest model...")
    return predict_all_tickers(last_30_days_df, historical_data, current_day_data, run_date=run_date)


def InsertPredictedValues(
    pgsql: PgSql,
    predictions_map: Dict[str, List[Dict[str, Any]]],
    prediction_date: Optional[str] = None,
) -> None:
    if not predictions_map:
        logger.warning("No predictions to insert.")
        return

    prediction_date = prediction_date or datetime.now().date().strftime("%Y-%m-%d")
    insert_query = """
        INSERT INTO predictions (ticker, forecast_date, predicted_close, prediction_date)
        VALUES (%s, %s, %s, %s)
    """

    try:
        inserted_count = 0
        for ticker, pred_list in predictions_map.items():
            for pred in pred_list:
                try:
                    pgsql.execute_query(
                        insert_query,
                        (
                            ticker,
                            pred["forecast_date"],
                            pred["predicted_close"],
                            prediction_date,
                        ),
                    )
                    inserted_count += 1
                except Exception as e:
                    logger.error(f"Error inserting prediction for {ticker} {pred['forecast_date']}: {str(e)}")
                    continue
        pgsql.commit()
        logger.info(f"Inserted {inserted_count} prediction rows into predictions table.")
    except psycopg2.Error as e:
        logger.error(f"Database error: {str(e)}")
        pgsql.rollback()
        raise


def main(run_date_override: str = None):
    if run_date_override:
        try:
            run_date = datetime.strptime(run_date_override.strip(), "%Y-%m-%d").date()
            logger.info(f"RunDate overridden via event: {run_date}")
        except ValueError:
            logger.error(f"Invalid RunDate format '{run_date_override}'. Expected YYYY-MM-DD. Using today.")
            run_date = datetime.now().date()
    else:
        run_date = datetime.now().date()
        logger.info(f"RunDate defaulting to today: {run_date}")

    pgsql = pgsql_from_environment()
    try:
        logger.info(f"Starting Stock Data Automation Pipeline (run_date={run_date})")

        pgsql.connect()

        ticker_map = fetchTickerList(pgsql)
        if not ticker_map:
            logger.error("No rows in tickers table.")
            return
        
        logger.info(f"Fetched {len(ticker_map)} tickers from tickers table.")
        
        current_day_data, historical_data = PullTickerData(ticker_map, run_date)

        if not current_day_data:
            logger.warning("No data was pulled. Exiting pipeline.")
            return

        InsertIntoRawDailyTable(pgsql, current_day_data)

        enriched_data = EnrichAndExtractFeatures(current_day_data, historical_data)
        if enriched_data:
            InsertIntoEnrichedDailyTable(pgsql, enriched_data)

        last_30_days_df = PullLast30DaysFromPostgres(pgsql, ticker_map, run_date)
        predictions_map = predictClosingPrice(last_30_days_df, historical_data, current_day_data, run_date)
        if predictions_map:
            InsertPredictedValues(pgsql, predictions_map, prediction_date=run_date.strftime("%Y-%m-%d"))

        logger.info("Stock Data Automation Pipeline Completed Successfully")

    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        raise
    finally:
        pgsql.close()


if __name__ == "__main__":
    main()
