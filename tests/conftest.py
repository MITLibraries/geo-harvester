import json
import pickle
from unittest.mock import patch

import boto3
import pytest
from click.testing import CliRunner
from moto import mock_s3

from harvester.aws.sqs import SQSClient, ZipFileEventMessage
from harvester.harvest import Harvester


@pytest.fixture(autouse=True)
def _test_env(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "None")
    monkeypatch.setenv("WORKSPACE", "test")
    monkeypatch.setenv("S3_RESTRICTED_CDN_ROOT", "s3://aws-account/cdn/geo/restricted/")
    monkeypatch.setenv("S3_PUBLIC_CDN_ROOT", "s3://aws-account/cdn/geo/public/")
    monkeypatch.setenv("GEOHARVESTER_SQS_TOPIC_NAME", "mocked-geo-harvester-input")


@pytest.fixture
def _unset_s3_cdn_env_vars(monkeypatch):
    monkeypatch.delenv("S3_RESTRICTED_CDN_ROOT")
    monkeypatch.delenv("S3_PUBLIC_CDN_ROOT")


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def generic_harvester_class():
    class GenericHarvester(Harvester):
        def full_harvest(self):
            raise NotImplementedError

        def incremental_harvest(self):
            raise NotImplementedError

    return GenericHarvester


@pytest.fixture
def mocked_restricted_bucket():
    bucket_name = "mocked_cdn_restricted"
    with mock_s3():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=bucket_name)
        yield bucket_name


@pytest.fixture
def mocked_restricted_bucket_empty(mocked_restricted_bucket):
    return mocked_restricted_bucket


@pytest.fixture
def mocked_restricted_bucket_one_legacy_fgdc_zip(mocked_restricted_bucket):
    with open(
        "tests/fixtures/s3_cdn_restricted_legacy_single/SDE_DATA_AE_A8GNS_2003.zip",
        "rb",
    ) as f:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=mocked_restricted_bucket,
            Key="cdn/geo/restricted/abc123.zip",
            Body=f.read(),
        )

    return mocked_restricted_bucket


@pytest.fixture
def mocked_sqs_topic_name():
    return "mocked-geo-harvester-input"


@pytest.fixture
def sqs_client_message_count_zero(mocked_sqs_topic_name):
    with patch.object(SQSClient, "get_message_count") as mocked_message_count:
        mocked_message_count.return_value = 0
        yield SQSClient(mocked_sqs_topic_name)


@pytest.fixture
def sqs_client_message_count_ten(mocked_sqs_topic_name):
    with patch.object(SQSClient, "get_message_count") as mocked_message_count:
        mocked_message_count.return_value = 10
        yield SQSClient(mocked_sqs_topic_name)


@pytest.fixture
def mock_boto3_sqs_client():
    with patch("harvester.aws.sqs.boto3.client") as mock_client:
        yield mock_client.return_value


@pytest.fixture
def mock_sqs_client(mocked_sqs_topic_name, mock_boto3_sqs_client):
    return SQSClient(mocked_sqs_topic_name)


@pytest.fixture
def _mocked_harvester_harvest():
    with patch.object(Harvester, "harvest") as mocked_harvest:
        mocked_harvest.return_value = None
        yield


@pytest.fixture
def invalid_sqs_message_dict():
    with open("tests/fixtures/sqs/invalid_message.json") as f:
        return json.loads(f.read())


@pytest.fixture
def valid_sqs_message_dict():
    with open("tests/fixtures/sqs/valid_message.json") as f:
        return json.loads(f.read())


@pytest.fixture
def valid_sqs_message_instance() -> ZipFileEventMessage:
    with open("tests/fixtures/sqs/valid_message.pickle", "rb") as f:
        # ruff: noqa: S301
        return pickle.load(f)
