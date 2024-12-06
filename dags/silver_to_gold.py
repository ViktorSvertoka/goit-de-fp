# Імпортуємо необхідні модулі з PySpark
from pyspark.sql import SparkSession 
from pyspark.sql.functions import avg, current_timestamp 
import os  

# Створюємо сесію Spark з ім'ям програми
spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

# Читаємо дані з паркетних файлів для таблиць "athlete_bio" та "athlete_event_results"
athlete_bio_df = spark.read.parquet("/tmp/silver/athlete_bio") 
athlete_event_results_df = spark.read.parquet("/tmp/silver/athlete_event_results") 

# Перейменовуємо стовпець "country_noc" на "bio_country_noc" у таблиці про атлетів
athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")

# Виконуємо об'єднання двох DataFrame за стовпцем "athlete_id"
joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

# Агрегуємо дані: обчислюємо середні значення для зросту та ваги, додаємо поточний час
aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),  
    avg("weight").alias("avg_weight"),  
    current_timestamp().alias("timestamp"),  
)

# Визначаємо шлях для збереження результатів у форматі Parquet
output_path = "/tmp/gold/avg_stats"
# Створюємо директорію, якщо її ще не існує
os.makedirs(output_path, exist_ok=True)

# Записуємо агреговані дані у форматі Parquet, перезаписуючи існуючі файли
aggregated_df.write.mode("overwrite").parquet(output_path)

# Виводимо повідомлення про успішне збереження даних
print(f"Data saved to {output_path}")

# Читаємо збережені дані і виводимо їх
df = spark.read.parquet(output_path)
df.show(truncate=False)

# Завершуємо роботу з сесією Spark
spark.stop()

