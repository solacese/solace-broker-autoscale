"""Cloud provisioning uses durable unique names, exact scope, gates and bounded retries."""

from types import SimpleNamespace

import httpx
import pytest

from solace_autoscale.actuator.safety import AuditLog
from solace_autoscale.assignment.store import AssignmentStore, Broker, BrokerState
from solace_autoscale.config import Config
from solace_autoscale.controller.provisioning import CloudProvisioner
from solace_autoscale.controller.store import ControllerStore
from solace_autoscale.metrics.fleet import FleetInventory

from . import test_measured_profiles as fixtures


@pytest.fixture
def measured_model():
    return fixtures.model.__wrapped__(fixtures.book.__wrapped__(), fixtures.limits.__wrapped__())


class Cloud:
    def __init__(self):
        self.calls = []
        self.services = []
        self.timeout = True

    def find_by_name_prefix(self, prefix):
        return [s for s in self.services if s["name"].startswith(prefix)]

    def create_service(self, body, idempotency_key):
        self.calls.append((body, idempotency_key))
        if self.timeout:
            raise httpx.ReadTimeout("unknown result")
        return "operation-id"


def controller(tmp_path, model):
    cfg = Config.model_validate(
        {
            "fleet": {"service_class": "enterprise-1k", "max_brokers": 4},
            "automation": {"enabled": True},
            "assignment": {"routing": "partitioned"},
            "actuation": {
                "mode": "scale-up-only",
                "dry_run": False,
                "require_confirmation": False,
                "kill_switch_file": str(tmp_path / "halt"),
            },
            "provisioning": {
                "enabled": True,
                "datacenter_id": "explicit-region",
                "broker_version": "10.1.2.3-0",
            },
        }
    )
    db = AssignmentStore(tmp_path / "state.db")
    db.upsert_broker(Broker("a", "payments", "vpn", BrokerState.ACTIVE, {"smf": "tcps://a"}))
    inv = FleetInventory.model_validate(
        {
            "provider": "aws",
            "broker_version": "10.1.2.3",
            "service_class": "enterprise-1k",
            "brokers": [
                {
                    "broker_id": "a",
                    "shard": "payments",
                    "msg_vpn": "vpn",
                    "base_url": "https://a.example",
                    "username_env": "U",
                    "password_env": "P",
                }
            ],
        }
    )
    return SimpleNamespace(
        config=cfg,
        model=model,
        assignments=db,
        inventory=inv,
        store=ControllerStore(db),
        queues=SimpleNamespace(connections={}, vpns={}),
    )


def test_uncertain_create_reuses_name_across_restart(tmp_path, measured_model):
    c = controller(tmp_path, measured_model)
    cloud = Cloud()
    audit = AuditLog(tmp_path / "audit.jsonl")
    p = CloudProvisioner(c, cloud, audit)
    p.reconcile(100, metrics_fresh=False)
    assert not cloud.calls
    p.reconcile(100, metrics_fresh=True)
    assert len(cloud.calls) == 1
    name = cloud.calls[0][0]["name"]
    p = CloudProvisioner(c, cloud, audit)
    p.reconcile(105, metrics_fresh=True)
    assert len(cloud.calls) == 1
    p.reconcile(161, metrics_fresh=True)
    assert len(cloud.calls) == 2
    assert cloud.calls[1][0]["name"] == name
    assert cloud.calls[1][1] == name
    assert cloud.calls[0][0]["serviceClassId"] == "ENTERPRISE_1K_HIGHAVAILABILITY"
    assert cloud.calls[0][0]["eventBrokerVersion"] == "10.1.2.3-0"
    cloud.services = [{"name": name, "id": "new-id", "creationState": "CREATING", "adminState": "START"}]
    assert "waiting" in p.reconcile(230, metrics_fresh=True)
    assert len(cloud.calls) == 2
    c.assignments.close()


def test_kill_switch_blocks_cloud_create(tmp_path, measured_model):
    c = controller(tmp_path, measured_model)
    cloud = Cloud()
    (tmp_path / "halt").touch()
    p = CloudProvisioner(c, cloud, AuditLog(tmp_path / "audit.jsonl"))
    assert "refused" in p.reconcile(100, metrics_fresh=True)
    assert not cloud.calls
    c.assignments.close()


def ready_detail(name):
    """Shape follows official Mission Control v2 Service/Broker/ConnectionEndpoint schemas."""
    return {
        'id': 'new-id', 'name': name, 'creationState': 'COMPLETED', 'adminState': 'START',
        'serviceClassId': 'ENTERPRISE_1K_HIGHAVAILABILITY', 'datacenterId': 'explicit-region',
        'eventBrokerServiceVersion': '10.1.2.3-0',
        'broker': {'version': '10.1.2.3', 'redundancyGroupSslEnabled': True, 'msgVpns': [{
            'msgVpnName': 'autoscale',
            'managementAdminLoginCredential': {'username': 'invented-user', 'password': 'invented-password'},
        }]},
        'serviceConnectionEndpoints': [{'hostNames': ['broker.example'], 'ports': [
            {'protocol': 'serviceManagementTlsListenPort', 'port': 943},
            {'protocol': 'serviceSmfTlsListenPort', 'port': 55443},
            {'protocol': 'serviceMqttTlsListenPort', 'port': 0},
        ]}],
    }


@pytest.mark.parametrize('field,value', [
    ('id', 'unrelated-id'), ('name', 'unrelated-service'), ('datacenterId', 'wrong-region'),
    ('eventBrokerServiceVersion', '10.9.0.0-0'), ('serviceClassId', 'DEVELOPER'),
])
def test_readiness_refuses_mismatched_service_before_semp(tmp_path, measured_model, field, value):
    c = controller(tmp_path, measured_model)
    cloud = Cloud()
    p = CloudProvisioner(c, cloud, AuditLog(tmp_path / 'audit.jsonl'))
    detail = ready_detail(p.prefix + 'test')
    row = {'name': detail['name'], 'shard': 'payments'}
    detail[field] = value
    cloud._get = lambda path: {'data': detail}
    service = {'id': 'new-id', 'creationState': 'COMPLETED', 'adminState': 'START'}
    with pytest.raises(ValueError, match='differs from its intent'):
        p._attach_ready(row, service)
    assert not c.queues.connections
    c.assignments.close()


def test_ready_service_attaches_after_semp_probe_and_ignores_disabled_ports(tmp_path, measured_model, monkeypatch):
    from solace_autoscale.controller import provisioning

    c = controller(tmp_path, measured_model)
    cloud = Cloud()
    p = CloudProvisioner(c, cloud, AuditLog(tmp_path / 'audit.jsonl'))
    detail = ready_detail(p.prefix + 'test')
    cloud._get = lambda path: {'data': detail}
    calls = []

    def respond(request):
        calls.append(request.method)
        return httpx.Response(200, json={'data': {}})

    real_client = httpx.Client
    monkeypatch.setattr(provisioning.httpx, 'Client', lambda **kw: real_client(
        **kw, transport=httpx.MockTransport(respond)))

    class Probe:
        def __init__(self, *args):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def collect(self, *args):
            calls.append('probe')

    monkeypatch.setattr(provisioning, 'SempCollector', Probe)
    monkeypatch.setenv('SOLACE_AUTOSCALE_CLIENT_PASSWORD', 'invented-client-password')
    assert p._attach_ready({'name': detail['name'], 'shard': 'payments'}, detail)
    assert calls == ['GET', 'PATCH', 'probe']
    broker = c.assignments.get_broker('new-id')
    assert broker.state == BrokerState.WARM
    assert broker.endpoints == {'smf': 'tcps://broker.example:55443'}
    c.assignments.close()
