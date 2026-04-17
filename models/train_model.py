"""Train an Isolation Forest anomaly detector and log it to MLflow."""

import os
import numpy as np
import pandas as pd
import joblib
import mlflow
import mlflow.sklearn
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

MODEL_PATH = os.path.join(os.path.dirname(__file__), "anomaly_model.pkl")


def generate_data(n: int = 10_000) -> pd.DataFrame:
    np.random.seed(42)
    normal = pd.DataFrame({
        "amount": np.random.normal(150, 50, n),
        "hour":   np.random.randint(0, 24, n),
        "count":  np.random.poisson(5, n),
    })
    anomalies = pd.DataFrame({
        "amount": np.random.uniform(5000, 20000, 200),
        "hour":   np.random.choice([2, 3], 200),
        "count":  np.random.randint(50, 200, 200),
    })
    return pd.concat([normal, anomalies], ignore_index=True)


def main():
    mlflow.set_experiment("anomaly-detection")

    with mlflow.start_run(run_name="isolation_forest"):
        df = generate_data()

        pipeline = Pipeline([
            ("scaler", StandardScaler()),
            ("model",  IsolationForest(
                n_estimators=100,
                contamination=0.02,
                random_state=42,
            )),
        ])
        pipeline.fit(df)

        preds        = pipeline.predict(df)
        anomaly_rate = (preds == -1).mean()

        mlflow.log_param("n_estimators",  100)
        mlflow.log_param("contamination", 0.02)
        mlflow.log_metric("anomaly_rate", round(anomaly_rate, 4))
        mlflow.sklearn.log_model(pipeline, "model")

        joblib.dump(pipeline, MODEL_PATH)
        print(f"Model saved  → {MODEL_PATH}")
        print(f"Anomaly rate : {anomaly_rate:.2%}")
        print("MLflow UI    : run 'mlflow ui' then open http://localhost:5000")


if __name__ == "__main__":
    main()
