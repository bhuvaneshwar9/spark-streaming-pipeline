"""
Local streaming demo — no Kafka or Docker needed.
Simulates a live event stream, detects anomalies, prints a live dashboard.

Run: python processor/local_stream_demo.py
"""

import os, sys, time, random, uuid
import numpy as np
import pandas as pd
from datetime import datetime
from collections import deque

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

CATEGORIES = ["electronics", "clothing", "food", "travel"]
REGIONS    = ["us-east", "us-west", "eu-central", "ap-south"]
BATCH_SIZE = 20
BATCHES    = 10

# ── Simple in-memory anomaly detector ────────────────────────────────────────
class AnomalyDetector:
    def __init__(self, window: int = 200):
        self.history = deque(maxlen=window)

    def fit(self, amounts):
        self.history.extend(amounts)

    def predict(self, amounts):
        if len(self.history) < 10:
            return [False] * len(amounts)
        mean = np.mean(self.history)
        std  = np.std(self.history) or 1
        return [abs(a - mean) > 3 * std for a in amounts]


# ── Event generator ───────────────────────────────────────────────────────────
def generate_batch(n: int) -> pd.DataFrame:
    amounts = [round(random.gauss(150, 50), 2) for _ in range(n)]
    # inject anomaly
    if random.random() < 0.15:
        idx = random.randint(0, n - 1)
        amounts[idx] = round(random.uniform(5000, 20000), 2)
    return pd.DataFrame({
        "id":        [str(uuid.uuid4())[:8] for _ in range(n)],
        "user_id":   [f"user_{random.randint(1,500)}" for _ in range(n)],
        "category":  [random.choice(CATEGORIES) for _ in range(n)],
        "region":    [random.choice(REGIONS) for _ in range(n)],
        "amount":    amounts,
        "timestamp": [datetime.utcnow().isoformat()] * n,
    })


# ── Dashboard printer ─────────────────────────────────────────────────────────
def print_batch_summary(batch_num: int, df: pd.DataFrame, anomalies: list):
    df["is_anomaly"] = anomalies
    flagged = df[df["is_anomaly"]]

    print(f"\n{'='*55}")
    print(f"  Batch {batch_num:02d}  |  {datetime.utcnow().strftime('%H:%M:%S')}  |  {len(df)} events")
    print(f"{'='*55}")
    print(f"  Total volume  : ${df['amount'].sum():>10,.2f}")
    print(f"  Avg amount    : ${df['amount'].mean():>10,.2f}")
    print(f"  Anomalies     : {len(flagged):>3} events flagged")

    if not flagged.empty:
        print(f"\n  ⚠  ANOMALIES DETECTED:")
        for _, row in flagged.iterrows():
            print(f"     {row['id']}  {row['user_id']:<12}  ${row['amount']:>10,.2f}  [{row['region']}]")

    print(f"\n  Top categories:")
    top = df.groupby("category")["amount"].sum().sort_values(ascending=False)
    for cat, val in top.items():
        print(f"    {cat:<15} ${val:,.2f}")


# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    print("\n" + "="*55)
    print("  SPARK STREAMING DEMO — LOCAL MODE")
    print("  Simulating real-time anomaly detection")
    print("="*55)

    detector    = AnomalyDetector()
    total_events = 0
    total_anomalies = 0

    for i in range(1, BATCHES + 1):
        batch = generate_batch(BATCH_SIZE)
        detector.fit(batch["amount"].tolist())
        anomalies = detector.predict(batch["amount"].tolist())
        print_batch_summary(i, batch, anomalies)
        total_events    += len(batch)
        total_anomalies += sum(anomalies)
        time.sleep(1)

    print(f"\n{'='*55}")
    print(f"  STREAM ENDED — {total_events} events, {total_anomalies} anomalies")
    print(f"{'='*55}\n")


if __name__ == "__main__":
    main()
