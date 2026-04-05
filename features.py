import pandas as pd
from typing import Optional

def lag_1(close_series: pd.Series) -> Optional[float]:
    if len(close_series) < 2:
        return None
    val = close_series.iloc[-2]
    return float(val) if pd.notna(val) else None


def lag_2(close_series: pd.Series) -> Optional[float]:
    if len(close_series) < 3:
        return None
    val = close_series.iloc[-3]
    return float(val) if pd.notna(val) else None


def lag_3(close_series: pd.Series) -> Optional[float]:
    if len(close_series) < 4:
        return None
    val = close_series.iloc[-4]
    return float(val) if pd.notna(val) else None


def lag_5(close_series: pd.Series) -> Optional[float]:
    if len(close_series) < 6:
        return None
    val = close_series.iloc[-6]
    return float(val) if pd.notna(val) else None


def lag_10(close_series: pd.Series) -> Optional[float]:
    if len(close_series) < 11:
        return None
    val = close_series.iloc[-11]
    return float(val) if pd.notna(val) else None


def ma_5(close_series: pd.Series) -> Optional[float]:
    if len(close_series) < 5:
        return None
    val = close_series.rolling(5).mean().iloc[-1]
    return float(val) if pd.notna(val) else None


def ma_10(close_series: pd.Series) -> Optional[float]:
    if len(close_series) < 10:
        return None
    val = close_series.rolling(10).mean().iloc[-1]
    return float(val) if pd.notna(val) else None


def ma_20(close_series: pd.Series) -> Optional[float]:
    if len(close_series) < 20:
        return None
    val = close_series.rolling(20).mean().iloc[-1]
    return float(val) if pd.notna(val) else None


def rolling_std_5(close_series: pd.Series) -> Optional[float]:
    if len(close_series) < 5:
        return None
    val = close_series.rolling(5).std().iloc[-1]
    return float(val) if pd.notna(val) else None


def rolling_std_10(close_series: pd.Series) -> Optional[float]:
    if len(close_series) < 10:
        return None
    val = close_series.rolling(10).std().iloc[-1]
    return float(val) if pd.notna(val) else None


def daily_return(close_series: pd.Series) -> Optional[float]:
    if len(close_series) < 2:
        return None
    prev = close_series.iloc[-2]
    curr = close_series.iloc[-1]
    if pd.isna(prev) or pd.isna(curr) or prev == 0:
        return None
    return float((curr - prev) / prev)


def return_5(close_series: pd.Series) -> Optional[float]:
    if len(close_series) < 6:
        return None
    prev = close_series.iloc[-6]
    curr = close_series.iloc[-1]
    if pd.isna(prev) or pd.isna(curr) or prev == 0:
        return None
    return float((curr - prev) / prev)


def return_10(close_series: pd.Series) -> Optional[float]:
    if len(close_series) < 11:
        return None
    prev = close_series.iloc[-11]
    curr = close_series.iloc[-1]
    if pd.isna(prev) or pd.isna(curr) or prev == 0:
        return None
    return float((curr - prev) / prev)


def return_20(close_series: pd.Series) -> Optional[float]:
    if len(close_series) < 21:
        return None
    prev = close_series.iloc[-21]
    curr = close_series.iloc[-1]
    if pd.isna(prev) or pd.isna(curr) or prev == 0:
        return None
    return float((curr - prev) / prev)


def ma_cross(close_series: pd.Series) -> Optional[float]:
    ma5 = ma_5(close_series)
    ma20 = ma_20(close_series)
    if ma5 is None or ma20 is None or ma20 == 0:
        return None
    return float(ma5 / ma20)
