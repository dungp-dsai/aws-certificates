import base64
import json
import os
import uuid
from datetime import datetime, timezone

import boto3
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from kafka import KafkaProducer
from kafka.net.sasl.oauth import AbstractTokenProvider

REGION = os.environ.get("AWS_REGION", "us-east-2")
BOOTSTRAP = os.environ["BOOTSTRAP_SERVERS"]
TOPIC = os.environ.get("TOPIC_NAME", "transactions")
ENDPOINT_NAME = os.environ.get("ENDPOINT_NAME", "")
SCORE_THRESHOLD = float(os.environ.get("SCORE_THRESHOLD", "0.5"))

_producer = None
_runtime = boto3.client("sagemaker-runtime")


class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(REGION)
        return token


def get_producer():
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=[server.strip() for server in BOOTSTRAP.split(",") if server.strip()],
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            sasl_oauth_token_provider=MSKTokenProvider(),
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8"),
            acks="all",
            retries=3,
            request_timeout_ms=10000,
        )
    return _producer


def score_amount(amount):
    if not ENDPOINT_NAME:
        return None, "not_scored"
    response = _runtime.invoke_endpoint(
        EndpointName=ENDPOINT_NAME,
        ContentType="application/json",
        Accept="application/json",
        Body=json.dumps({"amount": amount}).encode("utf-8"),
    )
    prediction = json.loads(response["Body"].read())
    score = float(prediction["score"])
    outcome = "review" if score >= SCORE_THRESHOLD else "approve"
    return score, outcome


def lambda_handler(event, context):
    raw = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode("utf-8")

    incoming = json.loads(raw)
    customer_id = str(incoming["customer_id"])
    transaction = {
        "transaction_id": incoming.get("transaction_id") or str(uuid.uuid4()),
        "event_timestamp": incoming.get("event_timestamp")
        or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "customer_id": customer_id,
        "merchant_id": str(incoming.get("merchant_id", "merch_unknown")),
        "amount": float(incoming["amount"]),
        "currency": incoming.get("currency", "USD"),
        "country": incoming.get("country", "US"),
        "card_present": bool(incoming.get("card_present", False)),
        "ip_address": incoming.get("ip_address", "203.0.113.10"),
    }
    transaction["model_score"], transaction["model_outcome"] = score_amount(transaction["amount"])

    get_producer().send(TOPIC, key=customer_id, value=transaction).get(timeout=10)

    return {
        "statusCode": 202,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(
            {
                "transaction_id": transaction["transaction_id"],
                "status": "accepted",
                "model_score": transaction["model_score"],
                "model_outcome": transaction["model_outcome"],
            }
        ),
    }
