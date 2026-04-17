# Real-Time Spark Streaming & Anomaly Detection

Real-time event pipeline using Kafka + PySpark Structured Streaming.
Detects anomalies with Isolation Forest, outputs results to console & JSON.

## Tech Stack
`Apache Kafka` `PySpark` `Scikit-learn` `MLflow` `Docker Compose`

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start Kafka (Docker required)
docker-compose up -d

# 3. Train anomaly detection model
python models/train_model.py

# 4. Start stream processor (terminal 1)
python processor/stream_processor.py

# 5. Start event producer (terminal 2)
python producer/kafka_producer.py
```

## Without Docker (pure Python demo)
```bash
python processor/local_stream_demo.py
```
Simulates the full pipeline in memory — no Kafka or Docker needed.

## Project Structure
```
spark-streaming-pipeline/
├── producer/
│   └── kafka_producer.py      # publishes synthetic events to Kafka
├── processor/
│   ├── stream_processor.py    # PySpark structured streaming job
│   └── local_stream_demo.py   # runs fully locally, no Kafka needed
├── models/
│   └── train_model.py         # trains Isolation Forest with MLflow
├── docker-compose.yml
└── requirements.txt
```
