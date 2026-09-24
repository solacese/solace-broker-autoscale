"""Process the configured durable subscriber group until interrupted."""

import os
import threading
from pathlib import Path

from customer_library import registry

from solace_autoscale_client import Message, MessagingClient


def credentials(_: str) -> tuple[str, str]:
    return os.environ["SOLACE_CLIENT_USERNAME"], os.environ["SOLACE_CLIENT_PASSWORD"]


def process(message: Message) -> None:
    # Replace this print with one transaction that stores message.event_id and the business effect.
    print(f"processed {message.event_id}: {message.topic} {message.payload}", flush=True)


with MessagingClient(
    os.environ.get("CONTROLLER_URL", "http://127.0.0.1:8099"),
    state_dir=Path(os.environ.get("SUBSCRIBER_STATE", ".state/subscriber")),
    credentials=credentials,
    api_key=os.environ.get("SOLACE_ASSIGNMENT_API_KEY"),
    routing_evaluators=registry,
) as client:
    client.subscribe(group="processor", handler=process, validate_routing=True)
    print("subscriber ready; press Ctrl-C to stop", flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
