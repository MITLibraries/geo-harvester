# ruff: noqa: N802, S301, SLF001

import json
from unittest.mock import MagicMock, patch

import boto3
import pytest
from click.testing import CliRunner
from moto import mock_s3

from harvester.aws.sqs import SQSClient, ZipFileEventMessage
from harvester.config import Config
from harvester.harvest import Harvester
from harvester.harvest.mit import MITHarvester
from harvester.records import FGDC, ISO19139, MITAardvark, Record, XMLSourceRecord


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
        def full_harvest_get_source_records(self):
            raise NotImplementedError

        def incremental_harvest_get_source_records(self):
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
def valid_sqs_message_deleted_dict():
    with open("tests/fixtures/sqs/valid_deleted_message.json") as f:
        return json.loads(f.read())


@pytest.fixture
def valid_sqs_message_deleted_instance() -> ZipFileEventMessage:
    with open("tests/fixtures/sqs/valid_deleted_message.json") as f:
        return ZipFileEventMessage(json.load(f))


@pytest.fixture
def valid_sqs_message_created_dict():
    with open("tests/fixtures/sqs/valid_created_message.json") as f:
        return json.loads(f.read())


@pytest.fixture
def valid_sqs_message_created_instance() -> ZipFileEventMessage:
    with open("tests/fixtures/sqs/valid_created_message.json") as f:
        return ZipFileEventMessage(json.load(f))


@pytest.fixture
def config_instance() -> Config:
    return Config()


@pytest.fixture
def valid_generic_xml_source_record():
    with open("tests/fixtures/records/generic/generic.xml", "rb") as f:
        return XMLSourceRecord(
            origin="mit",
            identifier="generic",
            metadata_format="fgdc",
            data=f.read(),
            event="created",
            nsmap={"plants": "http://example.com/plants"},
        )


@pytest.fixture
def valid_mit_iso19139_source_record():
    with open(
        "tests/fixtures/records/iso19139/in_bhopal_f7ward_2011.iso19139.xml", "rb"
    ) as f:
        return ISO19139(
            origin="mit",
            identifier="in_bhopal_f7ward_2011",
            data=f.read(),
            event="created",
        )


@pytest.fixture
def valid_mit_fgdc_source_record():
    with open("tests/fixtures/records/fgdc/SDE_DATA_AE_A8GNS_2003.xml", "rb") as f:
        return FGDC(
            origin="mit",
            identifier="SDE_DATA_AE_A8GNS_2003",
            data=f.read(),
            event="created",
        )


@pytest.fixture
def mocked_required_fields_source_record(valid_generic_xml_source_record):
    mocked_value = "Hello World!"

    class TestXMLSourceRecord(XMLSourceRecord):
        """Generic XMLSourceRecord

        Hardcoded methods for required fields for MITAardvark record
        """

        def _dct_accessRights_s(self):
            return mocked_value

        def _dct_title_s(self):
            titles = self.string_list_from_xpath("//plants:name[text() = 'Pink Lady']")
            return titles[0]

        def _gbl_mdModified_dt(self):
            return "2023-12-13T10:01:01.897520-05:00"

        def _gbl_mdVersion_s(self):
            return "Aardvark"

        def _gbl_resourceClass_sm(self):
            return ["Datasets", "Maps"]

        def _id(self):
            return mocked_value

        def _dcat_bbox(self):
            return mocked_value

        def _dct_references_s(self):
            return mocked_value

        def _locn_geometry(self):
            return mocked_value

    return TestXMLSourceRecord(
        origin="mit",
        identifier="generic",
        metadata_format="fgdc",
        data=valid_generic_xml_source_record.data,
        event=valid_generic_xml_source_record.event,
        nsmap=valid_generic_xml_source_record.nsmap,
    )


@pytest.fixture
def valid_mitaardvark_data_required_fields():
    return {
        "dct_accessRights_s": "value here",
        "dct_title_s": "value here",
        "gbl_mdModified_dt": "2023-12-13T10:01:01.897520-05:00",
        "gbl_mdVersion_s": "Aardvark",
        "gbl_resourceClass_sm": ["Datasets"],
        "id": "value here",
        "dcat_bbox": "value here",
        "dct_references_s": "value here",
        "locn_geometry": "value here",
    }


@pytest.fixture
def valid_mitaardvark_data_optional_fields(valid_mitaardvark_data_required_fields):
    valid_mitaardvark_data_required_fields.update(
        {
            "dcat_centroid": "value here",
        }
    )
    return valid_mitaardvark_data_required_fields


@pytest.fixture
def invalid_mitaardvark_data_required_fields():
    return {
        "dct_accessRights_s": None,
        "dct_title_s": "value here",
        "gbl_mdModified_dt": "2023-12-13",
        "gbl_mdVersion_s": "Invalid",
        "gbl_resourceClass_sm": ["Invalid"],
        "id": 1,
        "dcat_bbox": "value here",
        "dct_references_s": "value here",
        "locn_geometry": "value here",
    }


@pytest.fixture
def invalid_mitaardvark_data_optional_fields(invalid_mitaardvark_data_required_fields):
    invalid_mitaardvark_data_required_fields.update(
        {
            "dcat_centroid": 1,
        }
    )
    return invalid_mitaardvark_data_required_fields


@pytest.fixture
def valid_mitaardvark_record_required_fields(valid_mitaardvark_data_required_fields):
    return MITAardvark(**valid_mitaardvark_data_required_fields)


@pytest.fixture
def fgdc_source_record_from_zip():
    return MITHarvester.create_source_record_from_zip_file(
        identifier="SDE_DATA_AE_A8GNS_2003",
        event="created",
        zip_file="tests/fixtures/zip_files/SDE_DATA_AE_A8GNS_2003.zip",
    )


@pytest.fixture
def records_for_normalize(fgdc_source_record_from_zip):
    return [
        Record(
            identifier="SDE_DATA_AE_A8GNS_2003",
            source_record=fgdc_source_record_from_zip,
        )
    ]


@pytest.fixture
def fgdc_source_record_required_fields():
    identifier = "EG_CAIRO_A25TOPO_1972"
    with open("tests/fixtures/records/fgdc/fgdc_required_fields_only.xml", "rb") as f:
        return FGDC(
            identifier=identifier,
            origin="mit",
            event="created",
            data=f.read(),
        )


@pytest.fixture
def fgdc_source_record_all_fields():
    identifier = "SDE_DATA_US_P2HIGHWAYS_2005"
    with open("tests/fixtures/records/fgdc/fgdc_all_fields.xml", "rb") as f:
        return FGDC(
            identifier=identifier,
            origin="mit",
            event="created",
            data=f.read(),
        )


@pytest.fixture
def iso19139_source_record_required_fields():
    identifier = "def456"
    with open(
        "tests/fixtures/records/iso19139/iso19139_required_fields_only.xml", "rb"
    ) as f:
        return ISO19139(
            identifier=identifier,
            origin="mit",
            event="created",
            data=f.read(),
        )


@pytest.fixture
def iso19139_source_record_all_fields():
    identifier = "abc123"
    with open("tests/fixtures/records/iso19139/iso19139_all_fields.xml", "rb") as f:
        return ISO19139(
            identifier=identifier,
            origin="mit",
            event="created",
            data=f.read(),
        )


@pytest.fixture
def xpath_returns_nothing():
    with patch.object(XMLSourceRecord, "xpath_query") as mocked_xpath:
        mocked_xpath.return_value = []
        yield mocked_xpath


@pytest.fixture
def strings_from_xpath_unhandled_value():
    with patch.object(XMLSourceRecord, "string_list_from_xpath") as mocked_xpath_strings:
        mocked_xpath_strings.return_value = ["HIGHLY_UNUSUAL_VALUE_123"]
        yield mocked_xpath_strings


@pytest.fixture
def records_for_writing(fgdc_source_record_from_zip):
    record = Record(
        identifier="SDE_DATA_AE_A8GNS_2003",
        source_record=fgdc_source_record_from_zip,
    )
    record.normalized_record = record.source_record.normalize()
    return [record]


@pytest.fixture
def mocked_source_writer(generic_harvester_class):
    mock_source_writer = MagicMock()
    generic_harvester_class._write_source_metadata = mock_source_writer
    return mock_source_writer


@pytest.fixture
def mocked_normalized_writer(generic_harvester_class):
    mock_normalized_writer = MagicMock()
    generic_harvester_class._write_normalized_metadata = mock_normalized_writer
    return mock_normalized_writer
