"""PySpark Structured Streaming job — reads from Kafka, detects anomalies."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pandas as pd
import joblib, os

BROKER     = "localhost:9092"
TOPIC      = "transactions"
CHECKPOINT = "/tmp/checkpoint/transactions"
MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "models", "anomaly_model.pkl")

SCHEMA = StructType([
    StructField("id",        StringType()),
    StructField("user_id",   StringType()),
    StructField("category",  StringType()),
    StructField("region",    StringType()),
    StructField("amount",    DoubleType()),
    StructField("timestamp", StringType()),
])


def get_spark():
    return (SparkSession.builder
            .appName("AnomalyDetection")
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .getOrCreate())


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BROKER)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw
              .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d"))
              .select("d.*")
              .withColumn("timestamp", F.to_timestamp("timestamp"))
              .withColumn("processing_time", F.current_timestamp()))

    @F.pandas_udf("boolean")
    def is_anomaly(amounts: pd.Series) -> pd.Series:
        model = joblib.load(MODEL_PATH)
        X = pd.DataFrame({"amount": amounts, "hour": 12, "count": 1})
        return pd.Series(model.predict(X) == -1)

    enriched = parsed.withColumn("is_anomaly", is_anomaly(F.col("amount")))

    query = (enriched.writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", False)
             .option("checkpointLocation", CHECKPOINT)
             .start())

    query.awaitTermination()


if __name__ == "__main__":
    main()
