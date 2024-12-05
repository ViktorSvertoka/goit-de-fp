from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
import os

# Налаштування для роботи з Kafka через PySpark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# Налаштування для підключення до Kafka
kafka_config = {
    "bootstrap_servers": ["77.81.230.104:9092"],
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_jaas_config": (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        'username="admin" password="VawEzo1ikLtrA8Ug8THa";'
    ),
}

# Налаштування для підключення до MySQL
jdbc_config = {
    "url": "jdbc:mysql://217.61.57.46:3306/olympic_dataset",
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "driver": "com.mysql.cj.jdbc.Driver",
}

# Ініціалізація сесії Spark
spark = (
    SparkSession.builder.config("spark.jars", "mysql-connector-j-8.0.32.jar")
    .config("spark.sql.streaming.checkpointLocation", "checkpoint")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .appName("JDBCToKafka")
    .master("local[*]")
    .getOrCreate()
)

print("Spark version:", spark.version)

# Опис схеми для повідомлення Kafka
schema = StructType(
    [
        StructField("athlete_id", IntegerType(), True),
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)

# Читання даних з MySQL через JDBC
jdbc_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_config["url"],
        driver=jdbc_config["driver"],
        dbtable="athlete_event_results",
        user=jdbc_config["user"],
        password=jdbc_config["password"],
        partitionColumn="result_id",
        lowerBound=1,
        upperBound=1000000,
        numPartitions="10",
    )
    .load()
)

# Надсилання даних до Kafka
jdbc_df.selectExpr(
    "CAST(result_id AS STRING) AS key",
    "to_json(struct(*)) AS value",
).write.format("kafka").option(
    "kafka.bootstrap.servers", kafka_config["bootstrap_servers"]
).option(
    "kafka.security.protocol", kafka_config["security_protocol"]
).option(
    "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
).option(
    "kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"]
).option(
    "topic", "viktor_svertoka_athlete_event_results"
).save()

# Зчитування даних з Kafka
kafka_streaming_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"])
    .option("subscribe", "viktor_svertoka_athlete_event_results")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "5")
    .option("failOnDataLoss", "false")
    .load()
    .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
    .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.athlete_id", "data.sport", "data.medal")
)

# Читання біографічних даних атлетів з MySQL
athlete_bio_df = (
    spark.read.format("jdbc")
    .options(
        url=jdbc_config["url"],
        driver=jdbc_config["driver"],
        dbtable="athlete_bio",
        user=jdbc_config["user"],
        password=jdbc_config["password"],
        partitionColumn="athlete_id",
        lowerBound=1,
        upperBound=1000000,
        numPartitions="10",
    )
    .load()
)

# Фільтрація атлетів з валідними даними біографії
athlete_bio_df = athlete_bio_df.filter(
    (col("height").isNotNull())
    & (col("weight").isNotNull())
    & (col("height").cast("double").isNotNull())
    & (col("weight").cast("double").isNotNull())
)

# З'єднання потоку даних Kafka з біографічними даними атлетів
joined_df = kafka_streaming_df.join(athlete_bio_df, "athlete_id")

# Агрегування даних за спортом та медалями
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp"),
)


# Функція для запису результатів у Kafka та MySQL
def foreach_batch_function(df, epoch_id):

    df.selectExpr(
        "CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value"
    ).write.format("kafka").option(
        "kafka.bootstrap.servers", kafka_config["bootstrap_servers"]
    ).option(
        "kafka.security.protocol", kafka_config["security_protocol"]
    ).option(
        "kafka.sasl.mechanism", kafka_config["sasl_mechanism"]
    ).option(
        "kafka.sasl.jaas.config", kafka_config["sasl_jaas_config"]
    ).option(
        "topic", "viktor_svertoka_enriched_athlete_avg"
    ).save()

    # Запис в MySQL
    df.write.format("jdbc").options(
        url="jdbc:mysql://217.61.57.46:3306/neo_data",
        driver=jdbc_config["driver"],
        dbtable="viktor_svertoka_enriched_athlete_avg",
        user=jdbc_config["user"],
        password=jdbc_config["password"],
    ).mode("append").save()


# Запуск стрімінгової обробки
aggregated_df.writeStream.outputMode("complete").foreachBatch(
    foreach_batch_function
).option("checkpointLocation", "/path/to/checkpoint/dir").start().awaitTermination()
