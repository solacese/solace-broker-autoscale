"""Publish one event through the managed Python client."""

import os
import uuid
from pathlib import Path

from customer_library import registry

from solace_autoscale_client import MessagingClient


def credentials(_: str) -> tuple[str, str]:
    return os.environ["SOLACE_CLIENT_USERNAME"], os.environ["SOLACE_CLIENT_PASSWORD"]


with MessagingClient(
    os.environ.get("CONTROLLER_URL", "http://127.0.0.1:8099"),
    state_dir=Path(os.environ.get("PUBLISHER_STATE", ".state/publisher")),
    credentials=credentials,
    api_key=os.environ.get("SOLACE_ASSIGNMENT_API_KEY"),
    routing_evaluators=registry,
) as client:
    event_id = str(uuid.uuid4())
    client.publish(
        "orders/acme/created",
        {"order_id": "42", "amount": 85},
        event_id=event_id,
        headers={"tenant-id": "acme"},
    )
    if not client.flush(30):
        raise RuntimeError(f"event remains buffered: {client.status()}")
    print(f"broker accepted {event_id}")
