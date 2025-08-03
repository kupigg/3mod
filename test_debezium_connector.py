from confluent_kafka import Consumer

from utils import handle_kafka_message
from schema import OrdersSchema


topic_prefix = "customers"
users_table = "public.users"
orders_table = "public.orders"


def get_topic_name_from_psg(prefix_name: str, table_name: str):
    return f"{prefix_name}.{table_name}"


conf = {
    "bootstrap.servers": "localhost:9094",
    "group.id": "consumer-group",
    "auto.offset.reset": "earliest",
    # "enable.auto.commit": True,
    "session.timeout.ms": 6_000,
}
consumer = Consumer(conf)

test_topic_orders = get_topic_name_from_psg(topic_prefix, orders_table)
test_topic_users = get_topic_name_from_psg(topic_prefix, users_table)
consumer.subscribe([test_topic_orders])

try:
    while True:
        msg = consumer.poll(0.1)

        if msg is None:
            continue
        handle_kafka_message(msg)
finally:
    consumer.close()