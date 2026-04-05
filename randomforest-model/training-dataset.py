import yfinance as yf
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

ticker_map = {
    "AAPL": "Apple Inc.",
    "IBM": "International Business Machines",
    "MSFT": "Microsoft Corporation",
    "GOOG": "Alphabet Inc. (Google)",
    "AMZN": "Amazon.com, Inc.",
    "NFLX": "Netflix, Inc.",
    "TSLA": "Tesla, Inc.",
    "META": "Meta Platforms, Inc. (Facebook)",
    "NVDA": "NVIDIA Corporation",
    "ORCL": "Oracle Corporation"
}

bucket = os.environ.get("S3_BUCKET_NAME")
s3 = boto3.client('s3') if bucket else None

for ticker in ticker_map.keys():
    data = yf.download(ticker, period="50y", interval="1d")
    local_path = f"datasets/{ticker}_50y_daily.csv"
    data.to_csv(local_path)
    earliest_date = data.index.min()
    print(f"Generated Dataset for {ticker_map[ticker]} from {earliest_date.strftime('%Y-%m-%d')} to {data.index.max().strftime('%Y-%m-%d')}")

    if s3:
        s3_key = f"datasets/{ticker}_50y_daily.csv"
        s3.upload_file(local_path, bucket, s3_key)
        print(f"  Uploaded to s3://{bucket}/{s3_key}")
    print()
