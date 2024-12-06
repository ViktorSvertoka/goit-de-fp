# Імпортуємо необхідні модулі
from pyspark.sql import SparkSession
import os
import requests

# Функція для завантаження даних з FTP-сервера
def download_data(file):
  
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + file + ".csv" 
    print(f"Downloading from {downloading_url}")
    
    response = requests.get(downloading_url)

    if response.status_code == 200:
        with open(file + ".csv", "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {file}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")

# Створюємо сесію Spark для обробки даних
spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

# Список таблиць, які потрібно обробити
tables = ["athlete_bio", "athlete_event_results"]

# Проходимо по кожній таблиці в списку
for table in tables:
    local_path = f"{table}.csv"
    download_data(table)

    df = spark.read.csv(local_path, header=True, inferSchema=True)

    output_path = f"/tmp/bronze/{table}"
    os.makedirs(output_path, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)

    print(f"Data saved to {output_path}")
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

# Завершуємо сесію Spark
spark.stop()

