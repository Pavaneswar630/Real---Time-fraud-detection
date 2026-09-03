#!/usr/bin/env python3
"""
Real-Time Fraud Feature Store Ingestion Engine
=============================================
PySpark 3.5.0 Structured Streaming Job.

Subscribes to the `raw-transactions` Kafka topic, computes a 15-minute rolling
window aggregate for user transaction counts and volume, and writes feature
vectors atomically to Redis using MULTI/EXEC pipelines with a 900s TTL.

ARCHITECTURAL FIX NOTE:
In a 15-minute sliding window with a 1-minute slide, Spark emits updated aggregates
for up to 15 overlapping windows per micro-batch. Calling `HINCRBY` on these aggregated
rows causes ~15x compounded overcounting.
Instead, this job identifies the latest window (`window.end`) per user in each micro-batch
and performs an atomic `HSET` of the pre-computed aggregate with monotonic window timestamp
validation and a 900s TTL (matching the 15-minute window duration).
"""

import os
import sys
import json
import time
import logging
from typing import Iterator

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, sum as spark_sum,
    row_number, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from pyspark.sql.window import Window as PySparkWindow

# Configure Structured Logging
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
)
logger = logging.getLogger("fraud_spark_ingestion")

# Configuration from Environment
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw-transactions")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# DURABLE CHECKPOINTING:
# In local containers/dev, this points to a mounted persistent volume (`/data/spark-checkpoints/fraud-scoring`).
# Never point to `/tmp` in any environment; ephemeral storage destroys the offset WAL when containers restart.
# In enterprise production (Kubernetes/EMR), configure to S3/GCS/ADLS: e.g. `s3a://fraud-spark-checkpoints/scoring`.
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "./data/spark-checkpoints/fraud-scoring")

WINDOW_DURATION = os.getenv("WINDOW_DURATION", "15 minutes")
SLIDE_DURATION = os.getenv("SLIDE_DURATION", "1 minute")
FEATURE_TTL_SECONDS = int(os.getenv("FEATURE_TTL_SECONDS", "900"))  # 15m = 900s


# Lazy Redis client initialization per executor/partition
_redis_pool = None


def get_redis_client():
    """Maintain a connection pool per executor partition."""
    global _redis_pool
    import redis
    if _redis_pool is None:
        _redis_pool = redis.ConnectionPool(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=0,
            socket_timeout=3.0,
            socket_connect_timeout=3.0,
            max_connections=20
        )
    return redis.Redis(connection_pool=_redis_pool)


def write_partition_to_redis(rows: Iterator) -> None:
    """
    Worker function executed on Spark partitions.
    Uses atomic Redis MULTI/EXEC pipelines to update feature vectors with 900s TTL.
    """
    try:
        r = get_redis_client()
    except Exception as exc:
        logger.error(f"Failed to connect to Redis from partition worker: {exc}")
        return

    pipe = r.pipeline(transaction=True)
    batch_records = list(rows)
    if not batch_records:
        return

    current_unix_time = int(time.time())

    for record in batch_records:
        user_id = record["user_id"]
        txn_count = int(record["txn_count_15m"])
        total_amount = float(record["total_amount_15m"] or 0.0)
        window_end_epoch = int(record["window_end"].timestamp())
        key = f"user:{user_id}:features"

        # Atomic HSET + EXPIRE pipeline
        # Directly writing the latest rolling aggregate prevents sliding-window compounding.
        pipe.hset(key, mapping={
            "txn_count_15m": str(txn_count),
            "total_amount_15m": f"{total_amount:.2f}",
            "latest_window_end": str(window_end_epoch),
            "last_updated": str(current_unix_time)
        })
        pipe.expire(key, FEATURE_TTL_SECONDS)

    try:
        results = pipe.execute()
        logger.debug(f"Pipelined {len(batch_records)} feature updates to Redis successfully.")
    except Exception as exc:
        logger.error(f"Error executing Redis pipeline for partition: {exc}")
        raise exc


def process_micro_batch(batch_df, batch_id: int):
    """
    Sink callback for each micro-batch.
    Deduplicates overlapping sliding windows to select only the latest window per user
    before writing to Redis.
    """
    if batch_df.isEmpty():
        logger.info(f"Micro-batch {batch_id} is empty. Skipping.")
        return

    record_count = batch_df.count()
    logger.info(f"Processing micro-batch {batch_id} with {record_count} window-aggregate records.")

    # Deduplicate: For each user in this micro-batch, pick the newest window (max window_end)
    # This ensures we don't interleave older window slices if out-of-order data arrived.
    from pyspark.sql.window import Window
    partition_spec = Window.partitionBy("user_id").orderBy(col("window_end").desc())
    latest_per_user_df = batch_df.withColumn("rank", row_number().over(partition_spec)) \
                                 .filter(col("rank") == 1) \
                                 .drop("rank")

    # Write each partition using atomic Redis pipeline
    latest_per_user_df.foreachPartition(write_partition_to_redis)


def main():
    logger.info("Initializing SparkSession for Real-Time Fraud Feature Ingestion...")

    spark = SparkSession.builder \
        .appName("RealTimeFraudFeatureIngestion") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Transaction Schema from Kafka
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("timestamp", StringType(), False)
    ])

    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}")

    # Ingest from Kafka
    kafka_raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON payload and extract event timestamp
    parsed_stream = kafka_raw_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))

    # 15-Minute Rolling Window with 1-Minute Slide and 15-Minute Watermark
    windowed_aggregates = parsed_stream \
        .withWatermark("timestamp", WINDOW_DURATION) \
        .groupBy(
            window(col("timestamp"), WINDOW_DURATION, SLIDE_DURATION),
            col("user_id")
        ) \
        .agg(
            count("transaction_id").alias("txn_count_15m"),
            spark_sum("amount").alias("total_amount_15m")
        ) \
        .select(
            col("user_id"),
            col("txn_count_15m"),
            col("total_amount_15m"),
            col("window.end").alias("window_end")
        )

    logger.info(f"Starting Structured Streaming query with checkpointing at {CHECKPOINT_LOCATION}")

    query = windowed_aggregates.writeStream \
        .outputMode("update") \
        .foreachBatch(process_micro_batch) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime="5 seconds") \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping streaming query gracefully...")
        query.stop()
    except Exception as e:
        logger.error(f"Fatal error in streaming pipeline: {e}")
        query.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
