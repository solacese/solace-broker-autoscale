"""Small native topic application: run a consumer before publishing.

Install: pip install -e 'adapters/python[smf]'
Set CONTROLLER_URL, CONTROLLER_API_KEY, SOLACE_USERNAME and SOLACE_PASSWORD.
Run the configured controller/assignment service first, then:
  python examples/payments/native_app.py consume --state ./ledger-state --group ledger
  python examples/payments/native_app.py consume --state ./audit-state --group audit
  python examples/payments/native_app.py publish --state ./publisher-state --event-id payment-123
Use a separate persistent state directory for each process.
"""

from __future__ import annotations

import argparse
import json
import os
import threading

from solace_autoscale_client import Message, MessagingClient


def show(message: Message) -> None:
    """Demonstrate delivery; replace with an idempotent business transaction in production."""
    print(
        json.dumps({"event_id": message.event_id, "topic": message.topic, "payload": message.payload}),
        flush=True,
    )


def main() -> None:
    """Run a subscriber or durably publish a sample payment."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["consume", "publish"])
    parser.add_argument("--state", required=True)
    parser.add_argument("--event-id", default="payment-123")
    parser.add_argument("--group", default="ledger")
    args = parser.parse_args()
    user, password = os.environ["SOLACE_USERNAME"], os.environ["SOLACE_PASSWORD"]
    with MessagingClient(
        os.environ["CONTROLLER_URL"],
        state_dir=args.state,
        credentials=lambda _: (user, password),
        api_key=os.environ["CONTROLLER_API_KEY"],
    ) as client:
        if args.mode == "consume":
            client.subscribe(group=args.group, handler=show)
            try:
                threading.Event().wait()
            except KeyboardInterrupt:
                pass
        else:
            client.publish("payments/account-42/created", {"amount": 25}, event_id=args.event_id)
            if not client.flush(timeout=30):
                raise SystemExit("Not yet acknowledged; retained in the state directory. Restart to retry.")
            print("Broker acknowledged the payment.")


if __name__ == "__main__":
    main()
