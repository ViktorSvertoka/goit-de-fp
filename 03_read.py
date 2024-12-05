from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    TimestampType,
)

import os

# Налаштування для використання Kafka в Spark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# Параметри підключення до Kafka
kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

# Створення сесії Spark для обробки даних
spark = SparkSession.builder.appName("KafkaStreaming").master("local[*]").getOrCreate()

# Опис структури даних, що надходять у форматі JSON
schema = StructType(
    [
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("noc_country", StringType(), True),
        StructField("avg_height", StringType(), True),
        StructField("avg_weight", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

# Читання потокових даних із Kafka
kafka_streaming_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";',
    )
    .option("subscribe", "viktor_svertoka_enriched_athlete_avg")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "50")
    .option("failOnDataLoss", "false")
    .load()
    .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
    .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

# Виведення результатів потоку на консоль
kafka_streaming_df.writeStream.trigger(availableNow=True).outputMode("append").format(
    "console"
).option("truncate", "false").start().awaitTermination()
