#!/usr/bin/env python3
import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.net.sasl.oauth import AbstractTokenProvider

REGION = "us-east-2"
TOPIC = "transactions"
CUSTOMERS = [
    {"customer_id": "cust_001", "country": "US"},
    {"customer_id": "cust_002", "country": "US"},
    {"customer_id": "cust_003", "country": "DE"},
    {"customer_id": "cust_004", "country": "US"},
    {"customer_id": "cust_005", "country": "BR"},
]
MERCHANTS = ["merch_10", "merch_12", "merch_18", "merch_44"]
COUNTRIES = ["US", "DE", "BR", "NG"]


class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token


def build_event(customer=None, amount=None):
    customer = customer or random.choice(CUSTOMERS)
    if amount is None:
        amount = (
            round(random.uniform(450, 1200), 2)
            if random.random() < 0.25
            else round(random.uniform(8, 180), 2)
        )
    country = random.choice(COUNTRIES) if random.random() < 0.15 else customer["country"]
    return {
        "transaction_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "customer_id": customer["customer_id"],
        "merchant_id": random.choice(MERCHANTS),
        "amount": amount,
        "currency": "USD",
        "country": country,
        "card_present": random.random() < 0.3,
        "ip_address": f"203.0.113.{random.randint(1, 250)}",
    }


def kafka_kwargs(bootstrap):
    return {
        "bootstrap_servers": [server.strip() for server in bootstrap.split(",") if server.strip()],
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "OAUTHBEARER",
        "sasl_oauth_token_provider": MSKTokenProvider(),
    }


def ensure_topics(bootstrap):
    admin = KafkaAdminClient(**kafka_kwargs(bootstrap))
    existing = set(admin.list_topics())
    missing = [
        NewTopic(name=name, num_partitions=2, replication_factor=2)
        for name in ("transactions", "processed_transactions")
        if name not in existing
    ]
    if missing:
        admin.create_topics(missing)
    admin.close()


def consume(bootstrap, topic):
    consumer = KafkaConsumer(
        topic,
        group_id="fraud-lab-ec2",
        auto_offset_reset="earliest",
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        **kafka_kwargs(bootstrap),
    )
    for record in consumer:
        print(json.dumps(record.value))


def main():
    parser = argparse.ArgumentParser(description="Produce raw transactions to MSK")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--sleep", type=float, default=0.3)
    parser.add_argument("--burst-customer", default="cust_002")
    parser.add_argument("--burst", type=int, default=4)
    parser.add_argument("--consume", action="store_true")
    parser.add_argument("--topic", default=TOPIC)
    args = parser.parse_args()
    bootstrap = os.environ["BOOTSTRAP"]
    if args.consume:
        consume(bootstrap, args.topic)
        return
    ensure_topics(bootstrap)
    producer = KafkaProducer(
        **kafka_kwargs(bootstrap),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
    )

    def send(event):
        future = producer.send(TOPIC, key=event["customer_id"], value=event)
        future.get(timeout=10)
        print(f"{event['customer_id']} amount={event['amount']} country={event['country']}")

    for index in range(args.count):
        send(build_event())
        time.sleep(args.sleep)

    burst_customer = next(row for row in CUSTOMERS if row["customer_id"] == args.burst_customer)
    print(f"velocity burst for {args.burst_customer}")
    for _ in range(args.burst):
        send(build_event(customer=burst_customer, amount=round(random.uniform(15, 60), 2)))
        time.sleep(0.2)

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()