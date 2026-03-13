import sys
import types

from app.object_storage import S3ObjectStorage


def test_s3_object_storage_configures_boto3_client_timeouts(monkeypatch):
    captured = {}

    class FakeConfig:
        def __init__(self, **kwargs):
            captured["config_kwargs"] = kwargs

    class FakeBoto3Module:
        @staticmethod
        def client(service_name, *, region_name=None, config=None):
            captured["service_name"] = service_name
            captured["region_name"] = region_name
            captured["config"] = config
            return object()

    monkeypatch.setitem(sys.modules, "boto3", FakeBoto3Module())
    monkeypatch.setitem(
        sys.modules,
        "botocore.config",
        types.SimpleNamespace(Config=FakeConfig),
    )

    storage = S3ObjectStorage(bucket="archive-bucket", region="eu-west-2")

    assert storage.bucket == "archive-bucket"
    assert storage.region == "eu-west-2"
    assert captured["service_name"] == "s3"
    assert captured["region_name"] == "eu-west-2"
    assert captured["config_kwargs"] == {
        "connect_timeout": 10,
        "read_timeout": 60,
        "retries": {"max_attempts": 2},
        "max_pool_connections": 16,
    }
