# Імпортуємо необхідні модулі з PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import StringType
import os

# Створюємо сесію Spark з іменем "BronzeToSilver"
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# Функція для очищення текстових даних в датафреймах
def clean_text(df):
    
    for column in df.columns:
        
        if df.schema[column].dataType == StringType():
            
            df = df.withColumn(column, trim(lower(col(column))))
    return df

# Список таблиць, які потрібно обробити
tables = ["athlete_bio", "athlete_event_results"]

# Проходимо по кожній таблиці зі списку
for table in tables:
    
    df = spark.read.parquet(f"/tmp/bronze/{table}")

    df = clean_text(df)
    df = df.dropDuplicates()

   
    output_path = f"/tmp/silver/{table}"
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)

    
    print(f"Data saved to {output_path}")
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

# Завершуємо сесію Spark
spark.stop()

