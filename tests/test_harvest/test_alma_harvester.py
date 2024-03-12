"""tests.test_harvest.test_alma_harvester"""

# ruff: noqa: SLF001, PLR2004

import marcalyx
from lxml import etree


def marc_record_generator(file_path):
    with open(file_path, "rb") as file:
        yield marcalyx.Record(etree.fromstring(file.read()).getroottree().getroot())


def test_alma_harvester_list_local_xml_files(alma_harvester):
    alma_harvester.input_files = "tests/fixtures/alma/s3_folder"
    assert set(alma_harvester._list_local_xml_files()) == {
        "tests/fixtures/alma/s3_folder/alma-2023-12-31-daily-extracted-records-to-index_01.xml",
        "tests/fixtures/alma/s3_folder/alma-2024-01-01-daily-extracted-records-to-index_01.xml",
        "tests/fixtures/alma/s3_folder/alma-2023-12-31-full-extracted-records-to-index_01.xml",
        "tests/fixtures/alma/s3_folder/alma-2024-01-01-full-extracted-records-to-index_01.xml",
    }


def test_alma_harvester_list_s3_xml_files(alma_harvester):
    assert set(alma_harvester._list_s3_xml_files()) == {
        "s3://mocked-timdex-bucket/alma/alma-2023-12-31-daily-extracted-records-to-index_01.xml",
        "s3://mocked-timdex-bucket/alma/alma-2023-12-31-full-extracted-records-to-index_01.xml",
        "s3://mocked-timdex-bucket/alma/alma-2024-01-01-daily-extracted-records-to-index_01.xml",
        "s3://mocked-timdex-bucket/alma/alma-2024-01-01-full-extracted-records-to-index_01.xml",
    }


def test_alma_harvester_list_xml_files_filter_run_type(alma_harvester):
    # full harvest = "full" in filename
    alma_harvester.harvest_type = "full"
    assert set(alma_harvester._list_xml_files()) == {
        "s3://mocked-timdex-bucket/alma/alma-2024-01-01-full-extracted-records-to-index_01.xml",
    }
    # incremental harvest = "daily" in filename
    alma_harvester.harvest_type = "incremental"
    assert set(alma_harvester._list_xml_files()) == {
        "s3://mocked-timdex-bucket/alma/alma-2024-01-01-daily-extracted-records-to-index_01.xml",
    }


def test_alma_harvester_list_xml_files_filter_from_date(alma_harvester):
    alma_harvester.from_date, alma_harvester.until_date = "2024-01-01", None
    assert set(alma_harvester._list_xml_files()) == {
        "s3://mocked-timdex-bucket/alma/alma-2024-01-01-full-extracted-records-to-index_01.xml",
    }


def test_alma_harvester_list_xml_files_filter_until_date(alma_harvester):
    alma_harvester.from_date, alma_harvester.until_date = None, "2024-01-01"
    assert set(alma_harvester._list_xml_files()) == {
        "s3://mocked-timdex-bucket/alma/alma-2023-12-31-full-extracted-records-to-index_01.xml",
    }


def test_alma_harvester_list_xml_files_filter_from_to_until_dates(alma_harvester):
    alma_harvester.from_date, alma_harvester.until_date = "2023-12-01", "2024-02-01"
    assert set(alma_harvester._list_xml_files()) == {
        "s3://mocked-timdex-bucket/alma/alma-2023-12-31-full-extracted-records-to-index_01.xml",
        "s3://mocked-timdex-bucket/alma/alma-2024-01-01-full-extracted-records-to-index_01.xml",
    }


def test_alma_harvester_parse_marcalyx_marc_record_objects_from_xml_file(alma_harvester):
    record = next(alma_harvester.parse_marc_records_from_files())
    assert isinstance(record, marcalyx.Record)


def test_alma_harvester_filter_geospatial_records_count(alma_harvester):
    all_marc_records = alma_harvester.parse_marc_records_from_files()
    records = list(alma_harvester.filter_geospatial_marc_records(all_marc_records))
    assert len(records) == 2


def test_alma_harvester_filter_geospatial_fail_leader_code(alma_harvester):
    records_iter = marc_record_generator(
        "tests/fixtures/alma/single_records/geospatial_fail_leader.xml"
    )
    assert len(list(alma_harvester.filter_geospatial_marc_records(records_iter))) == 0


def test_alma_harvester_filter_geospatial_fail_655_tag(alma_harvester):
    records_iter = marc_record_generator(
        "tests/fixtures/alma/single_records/geospatial_fail_655.xml"
    )
    assert len(list(alma_harvester.filter_geospatial_marc_records(records_iter))) == 0


def test_alma_harvester_filter_geospatial_fail_949_tag(alma_harvester):
    records_iter = marc_record_generator(
        "tests/fixtures/alma/single_records/geospatial_fail_949.xml"
    )
    assert len(list(alma_harvester.filter_geospatial_marc_records(records_iter))) == 0


def test_alma_harvester_filter_geospatial_fail_985_tag(alma_harvester):
    records_iter = marc_record_generator(
        "tests/fixtures/alma/single_records/geospatial_fail_985.xml"
    )
    assert len(list(alma_harvester.filter_geospatial_marc_records(records_iter))) == 0


def test_alma_harvester_get_source_records_full(alma_harvester):
    alma_harvester.harvest_type = "full"
    records = list(alma_harvester.get_source_records())
    assert len(records) == 2


def test_alma_harvester_get_source_records_incremental(alma_harvester):
    alma_harvester.harvest_type = "incremental"
    records = list(alma_harvester.get_source_records())
    assert len(records) == 2
