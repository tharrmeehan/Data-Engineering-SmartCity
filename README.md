# Smart City Data Engineering

A real-time streaming pipeline that ingests simulated urban telemetry from a vehicle travelling London → Birmingham, processes five concurrent data streams with Apache Spark Structured Streaming, and writes the results to AWS S3 as partitioned Parquet files.

---

## Architecture

```
main.py  (Confluent Kafka producer)
    │
    ├─► vehicle_data      (location, speed, fuel type, heading)
    ├─► gps_data          (GPS coordinates, speed, vehicle type)
    ├─► traffic_data      (traffic camera snapshots, camera ID)
    ├─► weather_data      (temperature, humidity, wind, AQI)
    └─► emergency_data    (incident type, status, description)
          │
          ▼  Apache Kafka (Confluent Server 7.4.0 + Zookeeper)
          │
          ▼  spark-city.py  (PySpark Structured Streaming)
                │  readStream from each Kafka topic
                │  JSON schema enforcement per stream
                │  2-minute event-time watermarking
                │
                ▼  AWS S3   (Parquet, checkpointed)
```

All infrastructure is defined in `docker-compose.yml`: one Zookeeper node, one Confluent Kafka broker, one Spark master, and two Spark workers (2 cores / 2 GB each).

---

## Core Technical Stack

Python 3, Apache Kafka (Confluent Platform 7.4.0), Apache Spark 3.5 (PySpark), Spark Structured Streaming, AWS S3 (S3A connector), Docker Compose, confluent-kafka, simplejson

---

## Key Methodologies

- **Five typed Kafka topics** — each topic carries a distinct schema (vehicle telemetry, GPS, traffic camera, weather, emergency incidents). Schemas are enforced in PySpark using explicit `StructType` definitions, rejecting malformed records before they reach storage.

- **Event-time watermarking** — Spark applies a 2-minute watermark on the `timestamp` field of each stream. This bounds the state maintained for late-arriving records and prevents unbounded memory growth in long-running streaming jobs.

- **Confluent Kafka producer** — the vehicle simulation increments latitude/longitude incrementally (London → Birmingham) with Gaussian noise on each step, producing realistic spatial jitter. Five independent `produce()` calls fire per simulated time tick, keeping all topics in sync.

- **S3A Hadoop connector** — Spark writes each processed stream directly to S3 using the `s3a://` filesystem. AWS credentials are injected at runtime via environment variables, not baked into the image.

- **Checkpoint persistence** — each Spark stream maintains its own checkpoint directory on S3, enabling exactly-once delivery semantics and clean restart after failure.

---

## Production Metrics & Validation

- Two Spark workers at 2 cores / 2 GB each provide parallel stream processing across all five topics.
- Watermark window of 2 minutes calibrated to the simulated tick rate (30–60 seconds per event).
- Parquet output is schema-consistent per topic, enabling direct querying with Athena, Spark SQL, or DuckDB without a catalog layer.

---

## Local Replication

Prerequisites: Docker, Docker Compose, AWS credentials with S3 write access.

```bash
git clone https://github.com/tharrmeehan/Data-Engineering-SmartCity.git
cd Data-Engineering-SmartCity

# Configure credentials
cp jobs/config.py.example jobs/config.py   # add AWS_ACCESS_KEY, AWS_SECRET_KEY, S3 bucket
# (or set as environment variables before docker-compose up)

# Start the full stack
docker-compose up -d

# In a separate terminal — start the Kafka producer
pip install -r requirements.txt
python jobs/main.py

# Submit the Spark streaming job
docker exec spark-master \
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,\
org.apache.hadoop:hadoop-aws:3.3.1,\
com.amazonaws:aws-java-sdk:1.11.469 \
    /opt/bitnami/spark/jobs/spark-city.py
```

Spark UI is available at `http://localhost:9090`.

---

## Project Structure

```
├── docker-compose.yml         # Zookeeper, Kafka broker, Spark master + 2 workers
├── requirements.txt
└── jobs/
    ├── config.py              # AWS credentials and Kafka bootstrap config
    ├── main.py                # Confluent Kafka producer (5-topic vehicle simulation)
    └── spark-city.py          # PySpark Structured Streaming consumer → S3
```
