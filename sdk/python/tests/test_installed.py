from __future__ import annotations

import importlib.machinery
import importlib.metadata
from pathlib import Path

import dunnelean
from dunnelean import connectors, request_schema


def test_consumer_imports_an_installed_native_distribution(repo_root):
    distribution = importlib.metadata.distribution("dunnelean")
    assert distribution.version == dunnelean.__version__
    package = Path(dunnelean.__file__).resolve().parent
    source_root = repo_root / "sdk/python/python"
    assert source_root not in package.parents, "Install the built wheel before testing"
    assert not distribution.read_text("direct_url.json") or '"editable": true' not in distribution.read_text("direct_url.json")
    extensions = [
        path for path in package.iterdir()
        if any(path.name.endswith(suffix) for suffix in importlib.machinery.EXTENSION_SUFFIXES)
    ]
    assert extensions, "The installed package must contain its compiled Rust extension"


def test_schemas_are_available_without_a_service_or_database():
    schema = request_schema()
    assert schema["type"] == "object"
    assert {"reader", "writer", "execution"} <= schema["properties"].keys()
    catalog = connectors()
    assert {"connectors", "request_schema", "limits"} <= catalog.keys()
    assert catalog["request_schema"] == schema
