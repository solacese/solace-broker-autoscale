"""Wait for disposable loopback CI brokers to support queue operations, not just SEMP."""
from __future__ import annotations

import sys
import time
import uuid

import httpx


def wait(port: int) -> None:
    """Create/read/delete a unique test queue once the message spool is ready."""
    base = f"http://127.0.0.1:{port}/SEMP/v2/config/msgVpns/default/queues"
    name = "autoscale-readiness-" + uuid.uuid4().hex
    deadline = time.monotonic() + 90
    last = "broker unavailable"
    created = False
    with httpx.Client(auth=("admin", "admin"), timeout=5) as client:
        try:
            while time.monotonic() < deadline:
                try:
                    if not created:
                        response = client.post(base, json={
                            "queueName": name, "accessType": "exclusive", "permission": "consume",
                            "ingressEnabled": True, "egressEnabled": True, "maxMsgSpoolUsage": 1,
                        })
                        created = response.is_success
                        last = response.text
                    response = client.get(base + "/" + name)
                    if created and response.is_success and response.json()["data"]["ingressEnabled"]:
                        print(f"Broker {port}: message spool and durable queues ready", flush=True)
                        return
                    last = response.text
                except httpx.HTTPError as exc:
                    last = type(exc).__name__
                time.sleep(0.5)
            raise TimeoutError(f"Broker {port}: queue readiness failed: {last}")
        finally:
            if created:
                client.delete(base + "/" + name).raise_for_status()


if __name__ == "__main__":
    for argument in sys.argv[1:]:
        wait(int(argument))
