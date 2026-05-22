import os
import requests
import json
import time
import hmac
import hashlib
import base64
import pandas as pd
import boto3

from datetime import datetime, timezone, timedelta

# =====================================================
# API CONFIG
# =====================================================

API_KEY = os.getenv("ZODIA_API_KEY")

API_SECRET = os.getenv("ZODIA_API_SECRET")

BASE_URL = "https://trade-uk.zodiamarkets.com"
API_PATH = "api/3/transaction/list"

# =====================================================
# AWS S3 CONFIG
# =====================================================

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")

AWS_SECRET_ACCESS_KEY = os.getenv(
    "AWS_SECRET_ACCESS_KEY"
)

AWS_REGION = "ap-southeast-1"

S3_BUCKET = "payout-recon"
S3_KEY_PREFIX = "zodia/transactions/"

# =====================================================
# VALIDATE SECRETS
# =====================================================

required_env = {
    "ZODIA_API_KEY": API_KEY,
    "ZODIA_API_SECRET": API_SECRET,
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY
}

missing = [
    key for key, value in required_env.items()
    if not value
]

if missing:
    raise Exception(
        f"Missing environment variables: {', '.join(missing)}"
    )

# =====================================================
# DYNAMIC DATE RANGE (LAST 10 DAYS)
# =====================================================

today_utc = datetime.now(timezone.utc)

END_DATE = today_utc.strftime("%Y-%m-%d")

START_DATE = (
    today_utc - timedelta(days=9)
).strftime("%Y-%m-%d")

print("START_DATE:", START_DATE)
print("END_DATE:", END_DATE)

# =====================================================
# CONVERT DATE → TIMESTAMP
# =====================================================

start_ts = int(
    datetime.strptime(
        START_DATE,
        "%Y-%m-%d"
    ).replace(
        tzinfo=timezone.utc
    ).timestamp() * 1000
)

end_ts = int(
    datetime.strptime(
        END_DATE,
        "%Y-%m-%d"
    ).replace(
        tzinfo=timezone.utc
    ).timestamp() * 1000
)

# Include full end day
end_ts += 86399999

print("Start Timestamp:", start_ts)
print("End Timestamp:", end_ts)

# =====================================================
# SIGNATURE FUNCTION
# =====================================================

def generate_signature(path, body_str, secret):

    secret_bytes = base64.b64decode(secret)

    message = path + '\0' + body_str

    signature = hmac.new(
        secret_bytes,
        message.encode("utf-8"),
        hashlib.sha512
    ).digest()

    return base64.b64encode(signature).decode()

# =====================================================
# TIME CONVERSION
# =====================================================

def convert_millis(ms):

    if not ms:
        return "", ""

    ms = int(ms)

    # Convert seconds → millis
    if ms < 1000000000000:
        ms = ms * 1000

    dt = datetime.fromtimestamp(
        ms / 1000,
        tz=timezone.utc
    )

    utc_time = dt.strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )

    ist_time = dt.astimezone().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    return utc_time, ist_time

# =====================================================
# FETCH PAGINATED DATA
# =====================================================

all_transactions = []

offset = 0
limit = 100

while True:

    tonce = int(time.time() * 1000000)

    body = {
        "tonce": tonce,
        "limit": limit,
        "offset": offset
    }

    body_str = json.dumps(body)

    signature = generate_signature(
        API_PATH,
        body_str,
        API_SECRET
    )

    headers = {
        "Content-Type": "application/json",
        "Rest-Key": API_KEY,
        "Rest-Sign": signature
    }

    url = f"{BASE_URL}/{API_PATH}"

    response = requests.post(
        url,
        headers=headers,
        data=body_str,
        timeout=60
    )

    print(f"\nFetching Offset: {offset}")

    if response.status_code != 200:
        print("\nAPI Error:")
        print(response.text)
        break

    data = response.json()

    transactions = data.get("transactions", [])

    print("Transactions fetched:", len(transactions))

    if not transactions:
        print("No more transactions.")
        break

    matched_count = 0

    for txn in transactions:

        ts = (
            txn.get("processed")
            or txn.get("received")
            or txn.get("timestampMillis")
        )

        if not ts:
            continue

        ts = int(ts)

        if ts < 1000000000000:
            ts = ts * 1000

        if start_ts <= ts <= end_ts:
            all_transactions.append(txn)
            matched_count += 1

    print("Matched in this page:", matched_count)

    offset += limit

    time.sleep(0.2)

# =====================================================
# TRANSFORM DATA
# =====================================================

rows = []

for txn in all_transactions:

    received_utc, received_ist = convert_millis(
        txn.get("received")
    )

    processed_utc, processed_ist = convert_millis(
        txn.get("processed")
    )

    timestamp_utc, timestamp_ist = convert_millis(
        txn.get("timestampMillis")
    )

    rows.append({

        "uuid": txn.get("uuid"),

        "transactionClass": txn.get(
            "transactionClass"
        ),

        "transactionType": txn.get(
            "transactionType"
        ),

        "transactionState": txn.get(
            "transactionState"
        ),

        "amount": txn.get("amount"),

        "currency": txn.get("ccy"),

        "fee": txn.get("fee"),

        "displayTitle": txn.get(
            "displayTitle"
        ),

        "displayDescription": txn.get(
            "displayDescription"
        ),

        "tradeId": txn.get("tradeId"),

        "tradeRef": txn.get("tradeRef"),

        "customRef": txn.get("customRef"),

        "settleDate": txn.get("settleDate"),

        "received_utc": received_utc,
        "received_ist": received_ist,

        "processed_utc": processed_utc,
        "processed_ist": processed_ist,

        "timestamp_utc": timestamp_utc,
        "timestamp_ist": timestamp_ist,

        "paymentTransferType": txn.get(
            "paymentTransferType"
        ),

        "executedPrice": txn.get(
            "executedPrice"
        )

    })

# =====================================================
# CREATE DATAFRAME
# =====================================================

df = pd.DataFrame(rows)

print("\n====================================")
print("TOTAL MATCHED TRANSACTIONS:", len(df))
print("====================================")

# =====================================================
# EXPORT CSV
# =====================================================

file_name = (
    f"zodia_transactions_"
    f"{START_DATE}_to_{END_DATE}.csv"
)

df.to_csv(file_name, index=False)

print(f"\nCSV Saved: {file_name}")

# =====================================================
# UPLOAD CSV TO S3
# =====================================================

s3_key = f"{S3_KEY_PREFIX}{file_name}"

try:

    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    s3.upload_file(
        file_name,
        S3_BUCKET,
        s3_key
    )

    print("\n====================================")
    print("FILE UPLOADED TO S3 SUCCESSFULLY")
    print("====================================")

    print(
        f"S3 PATH: s3://{S3_BUCKET}/{s3_key}"
    )

except Exception as e:

    print("\n====================================")
    print("S3 Upload Failed")
    print("====================================")

    print(str(e))

    raise
