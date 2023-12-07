from unittest.mock import patch

from harvester.records.record import XMLSourceRecord


# ruff: noqa: SLF001
def test_iso19139_field_dct_title_s_one_returned(valid_mit_iso19139_source_record):
    assert (
        valid_mit_iso19139_source_record._dct_title_s()
        == "Bhopal, India (Ward Census Data, 2011)"
    )


def test_iso19139_field_dct_title_s_none_returned(valid_mit_iso19139_source_record):
    with patch.object(XMLSourceRecord, "xpath") as mocked_xpath:
        mocked_xpath.return_value = []
        assert valid_mit_iso19139_source_record._dct_title_s() is None
