import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import os
import glob
import tempfile
import boto3
from dotenv import load_dotenv

load_dotenv()

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATASETS_DIR = os.path.join(SCRIPT_DIR, 'datasets')
MODEL_PATH = os.path.join(SCRIPT_DIR, 'rf_model.joblib')

FEATURE_COLS = [
    'lag_1_ratio', 'lag_2_ratio', 'lag_3_ratio', 'lag_5_ratio', 'lag_10_ratio',
    'ma_5_ratio', 'ma_10_ratio', 'ma_20_ratio',
    'std_5_ratio', 'std_10_ratio',
    'daily_return', 'return_5', 'return_10', 'return_20',
    'ma_cross',
]

TARGET_COLS = [f'target_{h}' for h in range(1, 8)]

def load_and_prepare_ticker(csv_path):
    df = pd.read_csv(csv_path, skiprows=[1, 2])
    df = df.rename(columns={'Price': 'Date'})
    df['Date'] = pd.to_datetime(df['Date'])
    for col in ['Close', 'High', 'Low', 'Open', 'Volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.sort_values('Date').reset_index(drop=True)
    df = df.dropna(subset=['Close'])

    for lag in [1, 2, 3, 5, 10]:
        df[f'lag_{lag}'] = df['Close'].shift(lag)

    for win in [5, 10, 20]:
        df[f'ma_{win}'] = df['Close'].rolling(win).mean()

    for win in [5, 10]:
        df[f'rolling_std_{win}'] = df['Close'].rolling(win).std()

    df['daily_return'] = df['Close'].pct_change()
    for period in [5, 10, 20]:
        df[f'return_{period}'] = df['Close'].pct_change(period)

    for lag in [1, 2, 3, 5, 10]:
        df[f'lag_{lag}_ratio'] = df[f'lag_{lag}'] / df['Close']
    for win in [5, 10, 20]:
        df[f'ma_{win}_ratio'] = df[f'ma_{win}'] / df['Close']
    for win in [5, 10]:
        df[f'std_{win}_ratio'] = df[f'rolling_std_{win}'] / df['Close']

    df['ma_cross'] = df['ma_5'] / df['ma_20']

    for h in range(1, 8):
        df[f'target_{h}'] = df['Close'].shift(-h) / df['Close']

    df = df.dropna(subset=FEATURE_COLS + TARGET_COLS)

    for col in TARGET_COLS:
        df = df[(df[col] > 0.7) & (df[col] < 1.3)]

    return df[FEATURE_COLS + TARGET_COLS]


def _load_csv_files_from_s3(s3, bucket: str) -> list[tuple[str, str]] | None:
    response = s3.list_objects_v2(Bucket=bucket, Prefix="datasets/")
    keys = [
        obj['Key'] for obj in response.get('Contents', [])
        if obj['Key'].endswith('_50y_daily.csv')
    ]
    if not keys:
        return None

    results = []
    for key in sorted(keys):
        ticker = os.path.basename(key).split('_')[0]
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp:
            tmp_path = tmp.name
        s3.download_file(bucket, key, tmp_path)
        results.append((ticker, tmp_path))
    return results


def train():
    print("=" * 60)
    print("Random Forest Stock Price Predictor - Training")
    print("=" * 60)

    bucket = os.environ.get("S3_BUCKET_NAME")
    tmp_files = []
    all_data = []
    ticker_files = None

    if bucket:
        print(f"\nAttempting to load datasets from s3://{bucket}/datasets/")
        s3 = boto3.client('s3')
        ticker_files = _load_csv_files_from_s3(s3, bucket)
        if ticker_files:
            tmp_files = [path for _, path in ticker_files]
            print("Loaded from S3.\n")
        else:
            print(f"No datasets found in S3 — falling back to local {DATASETS_DIR}/\n")

    if not ticker_files:
        csv_paths = sorted(glob.glob(os.path.join(DATASETS_DIR, '*_50y_daily.csv')))
        if not csv_paths:
            raise FileNotFoundError(
                (
                    f"No datasets found in S3 (s3://{bucket}/datasets/) or locally ({DATASETS_DIR}). "
                    if bucket else
                    f"No CSV files found in {DATASETS_DIR}. "
                ) + "Run training-dataset.py first to generate them."
            )
        ticker_files = [(os.path.basename(p).split('_')[0], p) for p in csv_paths]
        print(f"Loading datasets from local {DATASETS_DIR}/\n")

    print(f"Found {len(ticker_files)} ticker datasets.\n")

    try:
        for ticker, csv_path in ticker_files:
            print(f"Loading {ticker}...", end=" ")
            ticker_data = load_and_prepare_ticker(csv_path)
            all_data.append(ticker_data)
            print(f"{len(ticker_data)} samples")
    finally:
        for path in tmp_files:
            os.unlink(path)

    df = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal training samples: {len(df)}")

    X = df[FEATURE_COLS].values
    Y = df[TARGET_COLS].values

    X_train, X_test, Y_train, Y_test = train_test_split(
        X, Y, test_size=0.2, random_state=42
    )
    print(f"Train: {len(X_train)}  |  Test: {len(X_test)}")

    print(f"\nTraining Random Forest model...")
    model = RandomForestRegressor(
        n_estimators=300,
        max_depth=20,
        min_samples_split=10,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, Y_train)

    Y_pred = model.predict(X_test)

    print(f"\nPer-horizon test-set performance:")
    print(f"{'Horizon':<12} {'MAE':>10} {'RMSE':>10} {'R²':>10}")
    print(f"{'-'*42}")
    for i, _ in enumerate(TARGET_COLS):
        mae = mean_absolute_error(Y_test[:, i], Y_pred[:, i])
        rmse = np.sqrt(mean_squared_error(Y_test[:, i], Y_pred[:, i]))
        r2 = r2_score(Y_test[:, i], Y_pred[:, i])
        print(f"Day+{i+1:<7} {mae:>10.6f} {rmse:>10.6f} {r2:>10.4f}")

    overall_r2 = r2_score(Y_test, Y_pred, multioutput='uniform_average')
    print(f"\nOverall R^2 (avg): {overall_r2:.4f}")

    print(f"\nFeature importances:")
    for name, imp in sorted(
        zip(FEATURE_COLS, model.feature_importances_), key=lambda x: -x[1]
    ):
        print(f"{name:20s} {imp:.4f}")

    with tempfile.NamedTemporaryFile(suffix='.joblib', delete=False) as tmp:
        tmp_path = tmp.name

    joblib.dump(model, tmp_path)

    s3_key = os.environ.get("MODEL_S3_KEY", "models/rf_model.joblib")

    if bucket:
        s3 = boto3.client('s3')
        s3.upload_file(tmp_path, bucket, s3_key)
        print(f"\nModel uploaded to s3://{bucket}/{s3_key}")
    else:
        print("S3_BUCKET_NAME not set — skipping S3 upload")

    os.unlink(tmp_path)
    print("=" * 60)


if __name__ == '__main__':
    train()
