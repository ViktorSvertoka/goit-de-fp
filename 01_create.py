from confluent_kafka.admin import AdminClient, NewTopic
from colorama import Fore, Style

kafka_config = {
    "bootstrap_servers": "77.81.230.104:9092",
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

# Налаштування клієнта
admin_client = AdminClient(
    {
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": kafka_config["security_protocol"],
        "sasl.mechanisms": kafka_config["sasl_mechanism"],
        "sasl.username": kafka_config["username"],
        "sasl.password": kafka_config["password"],
    }
)

my_name = "viktor_svertoka"
athlete_event_results = f"{my_name}_athlete_event_results"
enriched_athlete_avg = f"{my_name}_enriched_athlete_avg"
num_partitions = 2
replication_factor = 1

# Створення топіків
topics = [
    NewTopic(athlete_event_results, num_partitions, replication_factor),
    NewTopic(enriched_athlete_avg, num_partitions, replication_factor),
]

# Видалення топіків (опціонально, якщо вони існують)
try:
    delete_futures = admin_client.delete_topics(
        [athlete_event_results, enriched_athlete_avg]
    )
    for topic, future in delete_futures.items():
        try:
            future.result()  # Блокування до завершення видалення
            print(Fore.RED + f"Topic '{topic}' deleted successfully." + Style.RESET_ALL)
        except Exception as e:
            print(Fore.RED + f"Failed to delete topic '{topic}': {e}" + Style.RESET_ALL)
except Exception as e:
    print(Fore.RED + f"An error occurred during topic deletion: {e}" + Style.RESET_ALL)

# Створення топіків
try:
    create_futures = admin_client.create_topics(topics)
    for topic, future in create_futures.items():
        try:
            future.result()  # Блокування до завершення створення
            print(
                Fore.GREEN + f"Topic '{topic}' created successfully." + Style.RESET_ALL
            )
        except Exception as e:
            print(Fore.RED + f"Failed to create topic '{topic}': {e}" + Style.RESET_ALL)
except Exception as e:
    print(Fore.RED + f"An error occurred during topic creation: {e}" + Style.RESET_ALL)

# Список існуючих топіків
try:
    metadata = admin_client.list_topics(timeout=10)
    for topic in metadata.topics.keys():
        if my_name in topic:
            print(Fore.YELLOW + f"Topic '{topic}' already exists." + Style.RESET_ALL)
except Exception as e:
    print(Fore.RED + f"Failed to list topics: {e}" + Style.RESET_ALL)
