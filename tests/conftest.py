# ruff: noqa: N802, S301, SLF001, D202

import datetime
import glob
import json
import os
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
import pygit2
import pytest
import responses
from click.testing import CliRunner
from freezegun import freeze_time
from moto import mock_aws

from harvester.aws.sqs import SQSClient, ZipFileEventMessage
from harvester.config import Config
from harvester.harvest import Harvester
from harvester.harvest.alma import MITAlmaHarvester
from harvester.harvest.mit import MITHarvester
from harvester.harvest.ogm import OGMHarvester, OGMRepository
from harvester.records import (
    MITAardvark,
    Record,
    SourceRecord,
    XMLSourceRecord,
)
from harvester.records.formats import (
    FGDC,
    ISO19139,
)
from harvester.records.sources.ogm import OGMGBL1, OGMAardvark
from harvester.records.validators import ValidateGeoshapeWKT


@pytest.fixture(autouse=True)
def _test_env(monkeypatch):
    monkeypatch.setenv("SENTRY_DSN", "None")
    monkeypatch.setenv("WORKSPACE", "test")
    monkeypatch.setenv("S3_RESTRICTED_CDN_ROOT", "s3://aws-account/cdn/geo/restricted/")
    monkeypatch.setenv("S3_PUBLIC_CDN_ROOT", "s3://aws-account/cdn/geo/public/")
    monkeypatch.setenv("GEOHARVESTER_SQS_TOPIC_NAME", "mocked-geo-harvester-input")
    monkeypatch.setenv("OGM_CONFIG_FILEPATH", "tests/fixtures/ogm/ogm_test_config.yaml")
    monkeypatch.setenv("OGM_CLONE_ROOT_URL", "tests/fixtures/ogm/repositories")
    monkeypatch.setenv("OGM_CLONE_ROOT_DIR", "output/ogm")
    if "GITHUB_API_TOKEN" in os.environ:
        monkeypatch.delenv("GITHUB_API_TOKEN")


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
def mit_harvester_class():
    class GenericHarvester(MITHarvester):
        def full_harvest_get_source_records(self):
            raise NotImplementedError

        def incremental_harvest_get_source_records(self):
            raise NotImplementedError

    return GenericHarvester


@pytest.fixture
def mocked_restricted_bucket():
    bucket_name = "mocked_cdn_restricted"
    with mock_aws():
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

        def _dct_references_s(self):
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
def mocked_validated_fields_source_record():
    class TestValidatedSourceRecord:
        mocked_value = ""

        @ValidateGeoshapeWKT
        def _dcat_bbox(self):
            return self.mocked_value

        def _locn_geometry(self):
            return self._dcat_bbox()

    return TestValidatedSourceRecord


@pytest.fixture
def valid_mitaardvark_data_required_fields():
    return {
        "dct_accessRights_s": "value here",
        "dct_title_s": "value here",
        "gbl_mdModified_dt": "2023-12-13T10:01:01.897520-05:00",
        "gbl_mdVersion_s": "Aardvark",
        "gbl_resourceClass_sm": ["Datasets"],
        "id": "value here",
        "dct_references_s": "value here",
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
        "dct_references_s": "value here",
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
def generic_source_record():
    class GenericSourceRecord(SourceRecord):

        def _gbl_resourceType_sm(self):
            return []

    return GenericSourceRecord(
        origin="mit",
        identifier="abc123",
        metadata_format="fgdc",
        data=b"Nothing to see here.",
        event="created",
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
@freeze_time("2024-01-01")
def records_for_writing(fgdc_source_record_from_zip):
    record = Record(
        identifier="SDE_DATA_AE_A8GNS_2003",
        source_record=fgdc_source_record_from_zip,
    )
    record.normalized_record = record.source_record.normalize()
    return [record]


@pytest.fixture
def mocked_source_writer(mit_harvester_class):
    mock_source_writer = MagicMock()
    mit_harvester_class._write_source_metadata = mock_source_writer
    return mock_source_writer


@pytest.fixture
def mocked_normalized_writer(mit_harvester_class):
    mock_normalized_writer = MagicMock()
    mit_harvester_class._write_normalized_metadata = mock_normalized_writer
    return mock_normalized_writer


@pytest.fixture
def records_for_mit_steps(records_for_writing):
    return records_for_writing


@pytest.fixture
def mock_eventbridge_client(mock_boto3_sqs_client):
    with patch("harvester.aws.eventbridge.boto3.client") as mock_client:
        yield mock_client.return_value


@pytest.fixture
def ogm_config():
    return OGMRepository.load_repositories_config()


@pytest.fixture(autouse=True)
def mock_remote_repository_has_commits(request):
    if "use_github_api" not in request.keywords:
        with patch(
            "harvester.harvest.ogm.OGMRepository._remote_repository_has_new_commits"
        ) as mocked_method:
            mocked_method.return_value = True
            yield mocked_method
    else:
        yield


@pytest.fixture
def mocked_github_api_url():
    return "https://api.github.com/repos/OpenGeoMetadata/edu.earth/commits"


@pytest.fixture
def _mock_github_api_response_one_2010_commit(mocked_github_api_url):
    response = [
        {
            "sha": "94d2c8d2d34b41381fa3c80712f235788f5a1cd8",
            "commit": {"committer": {"date": "2010-01-01T00:00:00Z"}},
            "message": "I am a commit.",
        }
    ]
    responses.add(responses.GET, mocked_github_api_url, json=response, status=200)


@pytest.fixture
def _mock_github_api_response_zero_commits(mocked_github_api_url):
    responses.add(responses.GET, mocked_github_api_url, json=[], status=200)


@pytest.fixture
def _mock_github_api_response_404_not_found(mocked_github_api_url):
    responses.add(responses.GET, mocked_github_api_url, status=404)


@pytest.fixture
def _mock_github_api_response_403_rate_limit(mocked_github_api_url):
    """Generic mocked response to set custom .reason attribute on response"""

    class MockResponse:
        def __init__(self, reason, status_code):
            self.reason = reason
            self.status_code = status_code

    with patch("requests.get") as mocked_get:
        mocked_get.return_value = MockResponse("rate limit exceeded", 403)
        yield


@pytest.fixture
def ogm_repository_earth(ogm_config):
    return OGMRepository("edu.earth", ogm_config["edu.earth"])


@pytest.fixture
def ogm_repository_venus(ogm_config):
    return OGMRepository("edu.venus", ogm_config["edu.venus"])


@pytest.fixture
def ogm_repository_pluto(ogm_config):
    return OGMRepository("edu.pluto", ogm_config["edu.pluto"])


@pytest.fixture
def ogm_record_from_disk(ogm_repository_earth):
    return next(ogm_repository_earth.get_all_records())


@pytest.fixture
def ogm_record_from_git_history(ogm_repository_pluto):
    for record in ogm_repository_pluto.get_modified_records("2015-01-01"):
        if record.harvest_event == "deleted":
            return record
    return None


@pytest.fixture
def ogm_full_harvester():
    return OGMHarvester(harvest_type="full")


@pytest.fixture
def ogm_incremental_harvester():
    return OGMHarvester(harvest_type="incremental")


@pytest.fixture
def ogm_full_record_set():
    return {
        "edu.earth:5f5ac295b365",
        "edu.earth:3072f18cdeb5",
        "edu.venus:996864ca615e",
        "edu.venus:7fe1e637995f",
        "edu.pluto:83509b6d7e03",
        "edu.pluto:83fd37f6a879",
    }


def create_tz_date(year):
    return datetime.datetime(year=year, month=1, day=1, tzinfo=datetime.UTC)


def make_commit(repo, message, year):
    # create git author/committer
    person = pygit2.Signature(
        "Fake Person",
        "fakeperson@example.com",
        int(create_tz_date(year).timestamp()),
        0,
    )
    # create commit
    repo.create_commit(
        "refs/heads/main" if repo.head_is_unborn else repo.head.name,
        person,  # author
        person,  # committer
        message,
        repo.index.write_tree(),
        [] if repo.head_is_unborn else [repo.head.target],
    )


@pytest.fixture(scope="session")
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield Path(tmpdirname)


@pytest.fixture
def init_ogm_git_project_repos(
    monkeypatch,
    temp_dir,
    ogm_repository_earth,
    ogm_repository_venus,
    ogm_repository_pluto,
):
    """Fixture to build three git projects that simulate cloned OGM repositories.

    This avoids a complex situation where the git projects are created in advance in the
    main GeoHarvester project, but are difficult to git commit to the main project as they
    are themselves git projects with .git folders.  Looked into submodules and subtrees,
    but both would require additional work for installing the project and Github actions.

    Building these repositories as fixtures will also allow adding edge cases in the
    future if they crop up without manually modifying files and writing git commits
    in the simulated repositories.

    These repositories are built once to a temporary directory, and all subsequent calls
    to this fixture reuse them.  Looked into scope="session", but this did not play nicely
    with other fixtures used.
    """

    # set OGM clone root URL as the temp directory for this pytest session
    monkeypatch.setenv("OGM_CLONE_ROOT_URL", str(temp_dir))

    # define repo names
    repo_names = ["edu.earth", "edu.venus", "edu.pluto"]

    # if OGM repositories already created, yield to use
    if all(os.path.exists(temp_dir / repo_name) for repo_name in repo_names):
        yield None

    # else, build OGM repositories
    else:
        repos: dict[str, tuple[pygit2.Repository, Path]] = {}

        # create repository directory and initialize git project
        for repo_name in repo_names:
            repo_dir = temp_dir / repo_name
            repo_dir.mkdir()
            repos[repo_name] = (
                pygit2.init_repository(
                    str(repo_dir), bare=False, initial_head="refs/heads/main"
                ),
                repo_dir,
            )

        # create initial commit for all repos
        for repo, _repo_dir in repos.values():
            make_commit(repo, "Initial commit", 1990)

        # build edu.earth
        repo, repo_dir = repos["edu.earth"]
        files_dir = repo_dir / "gbl1"
        files_dir.mkdir()

        shutil.copy("tests/fixtures/ogm/files/edu.earth/record1.json", files_dir)
        repo.index.add("gbl1/record1.json")
        make_commit(repo, "First file commit", 2000)

        shutil.copy("tests/fixtures/ogm/files/edu.earth/record2.json", files_dir)
        repo.index.add("gbl1/record2.json")
        make_commit(repo, "Second file commit", 2010)

        # build edu.venus
        repo, repo_dir = repos["edu.venus"]
        files_dir = repo_dir / "aardvark"
        files_dir.mkdir()

        shutil.copy("tests/fixtures/ogm/files/edu.venus/record1.json", files_dir)
        repo.index.add("aardvark/record1.json")
        make_commit(repo, "First file commit", 2000)

        shutil.copy("tests/fixtures/ogm/files/edu.venus/record2.json", files_dir)
        repo.index.add("aardvark/record2.json")
        make_commit(repo, "Second file commit", 2010)

        # build edu.pluto
        repo, repo_dir = repos["edu.pluto"]
        files_dir = repo_dir / "fgdc"
        files_dir.mkdir()

        shutil.copy("tests/fixtures/ogm/files/edu.pluto/record1.xml", files_dir)
        repo.index.add("fgdc/record1.xml")
        make_commit(repo, "First file commit", 2000)

        shutil.copy("tests/fixtures/ogm/files/edu.pluto/record2.xml", files_dir)
        repo.index.add("fgdc/record2.xml")
        make_commit(repo, "Second file commit", 2010)

        os.remove(f"{files_dir}/record2.xml")
        repo.index.remove("fgdc/record2.xml")
        shutil.copy("tests/fixtures/ogm/files/edu.pluto/record3.xml", files_dir)
        repo.index.add("fgdc/record3.xml")
        make_commit(repo, "Removed second file and add third", 2020)

        yield None


@pytest.fixture
def mocked_ogm_harvester():
    with patch("harvester.cli.OGMHarvester") as mock_harvester:
        yield mock_harvester


@pytest.fixture
def gbl1_all_fields():
    with open("tests/fixtures/records/gbl1/gbl1_all_fields.json", "rb") as f:
        return OGMGBL1(
            origin="ogm",
            identifier="abc123",
            data=f.read(),
            event="created",
            ogm_repo_config={
                "name": "Earth",
                "metadata_format": "gbl1",
            },
        )


@pytest.fixture
def aardvark_all_fields():
    with open("tests/fixtures/records/aardvark/aardvark_all_fields.json", "rb") as f:
        return OGMAardvark(
            origin="ogm",
            identifier="abc123",
            data=f.read(),
            event="created",
            ogm_repo_config={
                "name": "Earth",
                "metadata_format": "aardvark",
            },
        )


@pytest.fixture
def mocked_timdex_bucket():
    bucket_name = "mocked-timdex-bucket"
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=bucket_name)
        yield bucket_name


@pytest.fixture
def mocked_timdex_alma_s3_export(mocked_timdex_bucket):
    for filepath in glob.glob("tests/fixtures/alma/s3_folder/*.*"):
        filename = filepath.split("/")[-1]
        with open(
            filepath,
            "rb",
        ) as f:
            s3 = boto3.client("s3")
            s3.put_object(
                Bucket=mocked_timdex_bucket,
                Key=f"alma/{filename}",
                Body=f.read(),
            )

    return mocked_timdex_bucket


@pytest.fixture
def alma_harvester(mocked_timdex_alma_s3_export):
    return MITAlmaHarvester(
        input_files="s3://mocked-timdex-bucket/alma",
        harvest_type="full",
        from_date="2024-01-01",
        until_date="2024-01-02",
    )
