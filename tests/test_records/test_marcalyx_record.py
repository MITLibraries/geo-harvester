"""tests.test_records.test_marcalyx_record"""

import pytest
from marcalyx.marcalyx import DataField

from harvester.records.sources.alma import AlmaMARC


def test_marcalyx_record_marc_property_parsed_on_init_from_data_alone():
    with open("tests/fixtures/alma/single_records/geospatial_valid.xml", "rb") as f:
        source_record = AlmaMARC(
            identifier="abc123",
            data=f.read(),
            event="created",
        )
    assert source_record.marc is not None


def test_marcalyx_record_get_single_tag_success(almamarc_source_record):
    assert isinstance(almamarc_source_record.get_single_tag("245"), DataField)


def test_marcalyx_record_get_single_tag_none_found(almamarc_source_record):
    assert almamarc_source_record.get_single_tag("1000") is None


def test_marcalyx_record_get_single_tag_multiple_error(almamarc_source_record):
    with pytest.raises(
        ValueError, match="Multiple tags found in MARC record for tag: 655"
    ):
        almamarc_source_record.get_single_tag("655")
