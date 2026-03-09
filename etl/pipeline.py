import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = os.getenv("DATABASE_URL", "postgresql://midas:midas123@localhost:5433/midasdb")

engine = create_engine(DB_URL)


def extract() -> tuple[pd.DataFrame, pd.DataFrame]:
    with engine.connect() as conn:
        users = pd.read_sql("SELECT * FROM user_record", conn)
        transactions = pd.read_sql(
            "SELECT * FROM transaction_record ORDER BY timestamp ASC", conn
        )
    return users, transactions


def transform(transactions: pd.DataFrame) -> pd.DataFrame:
    if transactions.empty:
        print("No transactions yet — skipping transform.")
        return transactions

    df = transactions.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")

    # 7-day rolling avg amount per sender
    df["avg_amount_7d"] = (
        df.groupby("sender_id")["amount"]
        .transform(lambda x: x.rolling("7D", on=df.loc[x.index, "timestamp"]).mean())
    )

    # Hourly transaction velocity per sender
    df["tx_velocity_1h"] = (
        df.groupby("sender_id")["amount"]
        .transform(lambda x: x.rolling("1h", on=df.loc[x.index, "timestamp"]).count())
    )

    # 30-day recipient diversity per sender
    df["recipient_diversity_30d"] = (
        df.groupby("sender_id")["recipient_id"]
        .transform(lambda x: x.rolling("30D", on=df.loc[x.index, "timestamp"]).apply(lambda w: w.nunique()))
    )

    return df


def load(df: pd.DataFrame) -> None:
    if df.empty:
        return
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS transaction_features (
                id BIGINT PRIMARY KEY,
                avg_amount_7d NUMERIC(19,4),
                tx_velocity_1h INTEGER,
                recipient_diversity_30d INTEGER
            )
        """))
        conn.commit()

    df[["id", "avg_amount_7d", "tx_velocity_1h", "recipient_diversity_30d"]].to_sql(
        "transaction_features",
        engine,
        if_exists="replace",
        index=False,
        method="multi"
    )
    print(f"Loaded {len(df)} rows into transaction_features.")


if __name__ == "__main__":
    print("Extracting...")
    users, transactions = extract()
    print(f"  {len(users)} users, {len(transactions)} transactions")

    print("Transforming...")
    features = transform(transactions)

    print("Loading...")
    load(features)
    print("ETL complete.")