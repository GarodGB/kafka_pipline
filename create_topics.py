"""Create the Kafka topics explicitly, using the values from the config."""
import json
import os
from dotenv import load_dotenv
load_dotenv()

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
with open(os.getenv("PIPELINE_CONFIG", "config/pipeline_config.json")) as f:
    k = json.load(f)["kafka"]

retention_ms = str(k["retention_days"] * 24 * 60 * 60 * 1000)   # 7 days -> 604800000 ms
admin = KafkaAdminClient(bootstrap_servers=KAFKA)

for name in [k["topic"], k["dead_letter_topic"]]:
    topic = NewTopic(name=name, num_partitions=k["partitions"], replication_factor=1,
                     topic_configs={"retention.ms": retention_ms})
    try:
        admin.create_topics([topic])
        print("created:", name, "| partitions:", k["partitions"], "| retention days:", k["retention_days"])
    except TopicAlreadyExistsError:
        print("already exists:", name)

admin.close()