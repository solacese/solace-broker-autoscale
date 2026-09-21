"""Shared Unicode and escaping vectors keep Go and Python on the same partition."""
import json
from pathlib import Path

from solace_autoscale_client.key_router import KeyRouter
from solace_autoscale_client.resolver import Resolver


def test_go_python_partition_vectors():
    vectors = json.loads((Path(__file__).parents[2] / "shim/testdata/managed-routing.json").read_text())
    router = KeyRouter(Resolver("http://unused"), "payments", "test", partitions=128)
    for vector in vectors:
        key = json.dumps([vector["account"]], separators=(",", ":"))
        assert key == vector["key"]
        assert router.partition_for(key) == vector["partition"]
