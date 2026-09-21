"""Long valid fleet IDs must fit Solace's 32-character client profile limit."""
import httpx

from solace_autoscale.actuator.solace_cloud import SempConnection
from solace_autoscale.controller.semp import QueueManager


def test_long_fleet_profile_is_valid_stable_and_isolated():
    names = []
    def respond(request):
        import json
        body = json.loads(request.content)
        name = body["clientProfileName"]
        assert len(name) <= 32
        names.append(name)
        return httpx.Response(200, json={})

    for fleet in ("payments-" * 6, "payments-" * 6, "analytics-" * 6):
        manager = QueueManager(fleet, {"a": SempConnection("http://broker", "u", "p")}, {"a": "default"})
        manager.client.close()
        manager.client = httpx.Client(transport=httpx.MockTransport(respond))
        manager.configure_native("a")
        manager.close()
    assert names[0] == names[1] == names[2] == names[3]
    assert names[4] == names[5] != names[0]
