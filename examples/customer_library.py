"""Customer-owned routing policy: keep one tenant's order lifecycle together."""

import hashlib

from solace_autoscale_client import Message, RoutingEvaluatorRegistry, SHA256Digest


def order_lifecycle(message: Message) -> SHA256Digest:
    if not isinstance(message.payload, dict):
        raise ValueError("payload must be an object")
    tenant = message.headers.get("tenant-id")
    order = message.payload.get("order_id")
    if not isinstance(tenant, str) or not tenant or "\x00" in tenant:
        raise ValueError("tenant-id header must be nonempty and contain no NUL")
    if not isinstance(order, str) or not order or "\x00" in order:
        raise ValueError("order_id must be nonempty and contain no NUL")
    encoded = b"".join(
        len(value.encode()).to_bytes(4, "big") + value.encode()
        for value in ("order-lifecycle-v1", tenant, order)
    )
    return SHA256Digest(hashlib.sha256(encoded).digest())


registry = RoutingEvaluatorRegistry().register(
    "order-lifecycle", "1.0.0", order_lifecycle
)


if __name__ == "__main__":
    first = Message("orders/acme/created", {"order_id": "42"}, "event-1", {"tenant-id": "acme"})
    next_step = Message("orders/acme/shipped", {"order_id": "42"}, "event-2", {"tenant-id": "acme"})
    other = Message("orders/acme/created", {"order_id": "43"}, "event-3", {"tenant-id": "acme"})
    assert order_lifecycle(first) == order_lifecycle(next_step)
    assert order_lifecycle(first) != order_lifecycle(other)
    print("routing policy OK: one order is stable and another order differs")
