#!/usr/bin/env python3
import argparse
import json
import os
import random
import time
import urllib.request
import uuid
from datetime import datetime, timezone

CUSTOMERS = [
    {"customer_id": "cust_001", "country": "US"},
    {"customer_id": "cust_002", "country": "US"},
    {"customer_id": "cust_003", "country": "DE"},
    {"customer_id": "cust_004", "country": "US"},
    {"customer_id": "cust_005", "country": "BR"},
]
MERCHANTS = ["merch_10", "merch_12", "merch_18", "merch_44"]
COUNTRIES = ["US", "DE", "BR", "NG"]


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


def post(url, event):
    payload = json.dumps(event).encode("utf-8")
    request = urllib.request.Request(
        url, data=payload, headers={"content-type": "application/json"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read().decode("utf-8")
        print(response.status, body)


def main():
    parser = argparse.ArgumentParser(description="Send synthetic transactions to API Gateway")
    parser.add_argument("--api-url", default=os.environ.get("API_URL"))
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--sleep", type=float, default=0.5)
    parser.add_argument("--burst-customer", default="cust_002")
    parser.add_argument("--burst", type=int, default=4)
    args = parser.parse_args()
    if not args.api_url:
        raise SystemExit("Set API_URL or pass --api-url")

    for index in range(args.count):
        event = build_event()
        print(
            f"send {index + 1}/{args.count} "
            f"customer={event['customer_id']} amount={event['amount']} country={event['country']}"
        )
        post(args.api_url, event)
        time.sleep(args.sleep)

    burst_customer = next(row for row in CUSTOMERS if row["customer_id"] == args.burst_customer)
    print(f"velocity burst for {args.burst_customer}")
    for index in range(args.burst):
        event = build_event(
            customer=burst_customer,
            amount=round(random.uniform(15, 60), 2),
        )
        print(f"burst {index + 1}/{args.burst} amount={event['amount']}")
        post(args.api_url, event)
        time.sleep(0.2)


if __name__ == "__main__":
    main()
