import json

from confluent_kafka import Message
from loguru import logger
from pydantic import BaseModel
from schema import OrdersSchema, UsersSchema


matching_serializers_to_topic = {
    "customers.public.orders.Key": OrdersSchema,
    "customers.public.users.Key": UsersSchema
}


def handle_kafka_message(message: Message):
    if message.error():
        print(f"Ошибка: {message.error()}")
        return
    key = json.loads(message.key().decode("utf-8"))
    value = message.value().decode("utf-8")
    serializer_msg = matching_serializers_to_topic.get(key["schema"]["name"])
    if serializer_msg:
        message_deserialize = serializer_msg(**json.loads(value)["payload"])
        logger.success(
            f"Получено сообщение: {key=}, {message_deserialize}, "
            f"partition={message.partition()}, offset={message.offset()}"
        )