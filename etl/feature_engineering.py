import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

DB_URL = "postgresql://midas:midas123@localhost:5433/midasdb"
engine = create_engine(DB_URL)

# ── LOAD DATA ─────────────────────────────────────────────────────────────────
txn_df = pd.read_sql("SELECT * FROM transaction_record ORDER BY timestamp", engine)
txn_df['timestamp'] = pd.to_datetime(txn_df['timestamp'])

print(f"Loaded {len(txn_df)} transactions")

# ── FEATURE ENGINEERING ───────────────────────────────────────────────────────
txn_df['hour_of_day']     = txn_df['timestamp'].dt.hour
txn_df['is_odd_hour']     = txn_df['hour_of_day'].apply(lambda h: 1 if h in [2, 3, 4] else 0)
txn_df['is_round_number'] = txn_df['amount'].apply(lambda a: 1 if a % 100 < 1 else 0)
txn_df['is_large_amount'] = txn_df['amount'].apply(lambda a: 1 if a > 450 else 0)

# Amount z-score (global)
txn_df['amount_zscore'] = (txn_df['amount'] - txn_df['amount'].mean()) / txn_df['amount'].std()

# 7-day rolling average amount per sender
txn_df = txn_df.sort_values('timestamp')
txn_df['avg_amount_7d'] = (
    txn_df.groupby('sender_id')['amount']
    .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
)

# Transaction velocity per sender (cumulative count)
txn_df['tx_velocity'] = txn_df.groupby('sender_id').cumcount()
txn_df['is_high_velocity'] = txn_df['tx_velocity'].apply(lambda v: 1 if v > 4 else 0)

# ── FRAUD LABELS ──────────────────────────────────────────────────────────────
def label_fraud(row):
    flags = 0
    if row['is_large_amount']:   flags += 1
    if row['is_round_number']:   flags += 1
    if row['is_odd_hour']:       flags += 1
    if row['is_high_velocity']:  flags += 1
    return 1 if flags >= 2 else 0

txn_df['is_fraud'] = txn_df.apply(label_fraud, axis=1)

print(f"Fraud transactions: {txn_df['is_fraud'].sum()} ({txn_df['is_fraud'].mean()*100:.1f}%)")

# ── SAVE TO DATABASE ──────────────────────────────────────────────────────────
features_df = txn_df[[
    'id', 'sender_id', 'recipient_id', 'amount',
    'hour_of_day', 'is_odd_hour', 'is_round_number', 'is_large_amount',
    'amount_zscore', 'avg_amount_7d', 'tx_velocity', 'is_high_velocity',
    'is_fraud'
]]

with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS transaction_features"))
    conn.commit()

features_df.to_sql('transaction_features', engine, if_exists='replace', index=False)
print(f"Saved {len(features_df)} rows to transaction_features table")

