# Імпортуємо необхідні модулі з Airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

# Визначаємо базовий шлях до файлів в DAG (якщо змінна оточення не задана, використовуємо за замовчуванням)
BASE_PATH = os.getenv("BASE_PATH", "/opt/airflow/dags")

# Задаємо значення за замовчуванням для аргументів DAG
default_args = {
    "owner": "airflow",  # Власник DAG
    "start_date": days_ago(1),  # Початкова дата для виконання DAG (1 день тому)
}

# Створюємо об'єкт DAG
dag = DAG(
    "viktor_svertoka_dag",
    default_args=default_args,  
    description="DAG by Viktor Svertoka", 
    schedule_interval=None,  
    tags=["viktor_svertoka"],
)

# Оператор для виконання скрипту landing_to_bronze.py
landing_to_bronze = BashOperator(
    task_id="landing_to_bronze",  
    bash_command=f"python {BASE_PATH}/landing_to_bronze.py",  
    dag=dag, 
)

# Оператор для виконання скрипту bronze_to_silver.py
bronze_to_silver = BashOperator(
    task_id="bronze_to_silver",
    bash_command=f"python {BASE_PATH}/bronze_to_silver.py",  
    dag=dag,  
)

# Оператор для виконання скрипту silver_to_gold.py
silver_to_gold = BashOperator(
    task_id="silver_to_gold", 
    bash_command=f"python {BASE_PATH}/silver_to_gold.py",  
    dag=dag,  
)

# Визначаємо порядок виконання задач
landing_to_bronze >> bronze_to_silver >> silver_to_gold

