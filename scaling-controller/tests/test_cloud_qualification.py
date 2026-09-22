"""Cloud qualification lifecycle is bounded, resumable, exact-ID scoped, and redacted."""

from __future__ import annotations

import json
import stat

import httpx
import pytest

from solace_autoscale.qualification.cloud import (
    QualificationPlan,
    QualificationRunner,
    RunJournal,
    redact,
)


def plan():
    return QualificationPlan.model_validate(
        {
            "status": "authorized",
            "region": "eks-us-east-1a",
            "service_class": "ENTERPRISE_100K_HIGHAVAILABILITY",
            "broker_version": "10.26.0.8894-14",
            "budget_eur": 2000,
            "billing_basis": "internal-non-billable-user-attested",
            "max_runtime_minutes": 30,
            "cleanup_timeout_minutes": 30,
            "delete_test_services_after_run": True,
            "service_count": 2,
            "services_created": [],
            "price_eur_per_hour": None,
            "purpose": "test",
            "requests": [
                {
                    "name": "autoscale-qualification-a",
                    "serviceClassId": "ENTERPRISE_100K_HIGHAVAILABILITY",
                    "datacenterId": "eks-us-east-1a",
                    "eventBrokerVersion": "10.26.0.8894-14",
                    "msgVpnName": "autoscale-test",
                    "redundancyGroupSslEnabled": True,
                    "locked": False,
                },
                {
                    "name": "autoscale-qualification-b",
                    "serviceClassId": "ENTERPRISE_100K_HIGHAVAILABILITY",
                    "datacenterId": "eks-us-east-1a",
                    "eventBrokerVersion": "10.26.0.8894-14",
                    "msgVpnName": "autoscale-test",
                    "redundancyGroupSslEnabled": True,
                    "locked": False,
                },
            ],
        }
    )


class Cloud:
    def __init__(self):
        self.services = []
        self.created = []
        self.deleted = []

    def list_services(self):
        return list(self.services)

    def get_datacenter(self, datacenter_id):
        return {"data": {"id": datacenter_id, "available": True,
                         "supportedServiceClasses": ["ENTERPRISE_100K_HIGHAVAILABILITY"]}}

    def list_service_versions(self, datacenter_id):
        return {"data": [{"version": "10.26.0.8894-14", "recommended": True,
                           "supportedServiceClasses": ["ENTERPRISE_100K_HIGHAVAILABILITY"]}]}

    def list_service_classes(self):
        return {"data": [{"id": "ENTERPRISE_100K_HIGHAVAILABILITY"}]}

    def create_service_request(self, body, key):
        index = len(self.created) + 1
        service_id = f"svc-{index}"
        operation_id = f"create-{index}"
        self.created.append((body, key, operation_id, service_id))
        service = {"id": service_id, "name": body["name"], **body,
                   "creationState": "COMPLETED", "adminState": "START",
                   "allowedActions": ["delete"]}
        service["eventBrokerServiceVersion"] = service.pop("eventBrokerVersion")
        self.services.append(service)
        return {"data": {"id": operation_id, "resourceId": service_id, "status": "PENDING"}}

    def delete_service(self, service_id, key):
        self.deleted.append((service_id, key))
        return "delete-" + service_id

    def get_operation(self, operation_id):
        if operation_id.startswith("create-"):
            index = int(operation_id.split("-")[1]) - 1
            return {"data": {"id": operation_id, "status": "SUCCEEDED",
                             "resourceId": self.created[index][3]}}
        return {"data": {"id": operation_id, "status": "SUCCEEDED"}}

    def get_service_operation(self, service_id, operation_id):
        return {"data": {"id": operation_id, "resourceId": service_id, "status": "SUCCEEDED"}}

    @staticmethod
    def operation_status(body):
        from solace_autoscale.cloud import OperationStatus
        return OperationStatus(body["data"]["status"])

    def get_service(self, service_id):
        for service in self.services:
            if service["id"] == service_id:
                if any(item[0] == service_id for item in self.deleted):
                    request = httpx.Request("GET", "https://api.example/service")
                    response = httpx.Response(404, request=request)
                    raise httpx.HTTPStatusError("gone", request=request, response=response)
                return {"data": service}
        request = httpx.Request("GET", "https://api.example/service")
        response = httpx.Response(404, request=request)
        raise httpx.HTTPStatusError("gone", request=request, response=response)


def test_journal_is_private_and_atomic(tmp_path):
    journal = RunJournal(tmp_path / "journal.json", plan(), now=lambda: 100)
    journal.event("safe", token="[REDACTED]")
    assert stat.S_IMODE(journal.path.stat().st_mode) == 0o600
    assert json.loads(journal.path.read_text())["events"][0]["event"] == "safe"
    assert not journal.path.with_suffix(".json.tmp").exists()


def test_create_and_cleanup_use_only_journaled_exact_ids(tmp_path):
    cloud = Cloud()
    journal = RunJournal(tmp_path / "journal.json", plan(), now=lambda: 100)
    runner = QualificationRunner(plan(), journal, cloud, sleep=lambda _: None, clock=lambda: 101)
    assert runner.create_all() == ("svc-1", "svc-2")
    assert journal.created_ids() == ("svc-1", "svc-2")
    result = runner.cleanup()
    assert result.deleted == 2 and not result.remaining
    assert [item[0] for item in cloud.deleted] == ["svc-1", "svc-2"]


def test_preflight_refuses_preexisting_target_name(tmp_path):
    cloud = Cloud()
    cloud.services.append({"id": "foreign", "name": "autoscale-qualification-a"})
    runner = QualificationRunner(
        plan(), RunJournal(tmp_path / "journal.json", plan(), now=lambda: 100), cloud,
        sleep=lambda _: None, clock=lambda: 101,
    )
    with pytest.raises(ValueError, match="already exists"):
        runner.preflight()
    assert not cloud.deleted


def test_uncertain_create_is_reconciled_only_after_attempt_was_journaled(tmp_path):
    cloud = Cloud()
    journal = RunJournal(tmp_path / "journal.json", plan(), now=lambda: 100)
    record = journal.service("autoscale-qualification-a")
    record["create_attempted_at"] = "test"
    journal.save()
    cloud.services.append({"id": "svc-own", "name": "autoscale-qualification-a",
                           "serviceClassId": "ENTERPRISE_100K_HIGHAVAILABILITY",
                           "datacenterId": "eks-us-east-1a",
                           "eventBrokerServiceVersion": "10.26.0.8894-14", "locked": False,
                           "creationState": "COMPLETED", "adminState": "START",
                           "allowedActions": ["delete"]})
    runner = QualificationRunner(plan(), journal, cloud, sleep=lambda _: None, clock=lambda: 101)
    runner.preflight()
    assert journal.created_ids() == ("svc-own",)


def test_cleanup_uses_separate_deadline_after_main_run_timeout(tmp_path):
    cloud = Cloud()
    journal = RunJournal(tmp_path / "journal.json", plan(), now=lambda: 0)
    record = journal.service("autoscale-qualification-a")
    record.update(service_id="svc-1", create_attempted_at="test")
    cloud.services.append({"id": "svc-1", "name": record["name"]})
    journal.save()
    runner = QualificationRunner(plan(), journal, cloud, sleep=lambda _: None, clock=lambda: 10**9)
    result = runner.cleanup()
    assert result.deleted == 1 and not result.remaining


def test_cleanup_records_failure_without_deleting_unjournaled_service(tmp_path):
    class FailingCloud(Cloud):
        def delete_service(self, service_id, key):
            raise httpx.ConnectError("offline")

    cloud = FailingCloud()
    cloud.services.extend([{"id": "owned", "name": "autoscale-qualification-a"},
                           {"id": "existing", "name": "customer-service"}])
    journal = RunJournal(tmp_path / "journal.json", plan(), now=lambda: 100)
    journal.service("autoscale-qualification-a").update(
        service_id="owned", create_attempted_at="test"
    )
    journal.save()
    result = QualificationRunner(plan(), journal, cloud, clock=lambda: 101).cleanup()
    assert result.remaining == ("owned",)
    assert not cloud.deleted


def test_redaction_removes_ids_hosts_credentials_and_payloads():
    value = {
        "service_id": "svc-secret",
        "hostname": "broker.example",
        "username": "alice",
        "password": "secret",
        "token": "bearer",
        "payload": {"card": "private"},
        "safe": 7,
    }
    result = redact(value)
    serialized = json.dumps(result)
    assert "svc-secret" not in serialized
    assert "broker.example" not in serialized
    assert "alice" not in serialized
    assert "secret" not in serialized
    assert "private" not in serialized
    assert result["safe"] == 7
