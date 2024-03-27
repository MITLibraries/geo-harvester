"""tests.test_records.test_marcalyx_record"""

import pytest
from marcalyx.marcalyx import DataField, SubField

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


def test_marcalyx_record_get_single_subfield_success(almamarc_source_record):
    tag = almamarc_source_record.get_single_tag("245")
    assert isinstance(almamarc_source_record.get_single_subfield(tag, "a"), SubField)


def test_marcalyx_record_get_single_subfield_none_found(almamarc_source_record):
    tag = almamarc_source_record.get_single_tag("245")
    assert almamarc_source_record.get_single_subfield(tag, "x") is None


def test_marcalyx_record_get_single_subfield_multiple_error(almamarc_source_record):
    tag = almamarc_source_record.get_single_tag("969")
    with pytest.raises(
        ValueError, match="Multiple subfields found in tag for subfield: a"
    ):
        almamarc_source_record.get_single_subfield(tag, "a")


def test_marcalyx_record_get_single_tag_and_subfield_value_success(
    almamarc_source_record,
):
    assert almamarc_source_record.get_single_tag_subfield_value("245", "a") == "Bahrain"


def test_marcalyx_record_get_single_tag_and_subfield_value_no_tag_error(
    almamarc_source_record,
):
    with pytest.raises(
        ValueError, match="Record does not have single instance of tag '999'"
    ):
        assert almamarc_source_record.get_single_tag_subfield_value("999", "x") == "apple"


def test_marcalyx_record_get_single_tag_and_subfield_value_no_subfield_error(
    almamarc_source_record,
):
    with pytest.raises(
        ValueError, match="Tag does not have single instance of subfield 'x'"
    ):
        assert almamarc_source_record.get_single_tag_subfield_value("245", "x") == "apple"


def test_marcalyx_record_get_single_tag_and_subfield_value_multiple_tag_error(
    almamarc_source_record,
):
    with pytest.raises(ValueError, match="Multiple tags found"):
        assert almamarc_source_record.get_single_tag_subfield_value("655", "a") == "Maps"


def test_marcalyx_record_get_multiple_tag_subfield_values_no_concat_success(
    almamarc_source_record,
):
    assert almamarc_source_record.get_multiple_tag_subfield_values(
        [("245", "a"), ("994", "ab")]
    ) == ["Bahrain", "02", "MYG"]


def test_marcalyx_record_get_multiple_tag_subfield_values_no_concat_empty(
    almamarc_source_record,
):
    assert almamarc_source_record.get_multiple_tag_subfield_values([("999", "x")]) == []


def test_marcalyx_record_get_multiple_tag_subfield_values_concat_success(
    almamarc_source_record,
):
    assert almamarc_source_record.get_multiple_tag_subfield_values(
        [("245", "a"), ("994", "ab")],
        concat=True,
    ) == ["Bahrain", "02 MYG"]


def test_marcalyx_record_get_multiple_tag_subfield_values_concat_custom_seperator(
    almamarc_source_record,
):
    assert almamarc_source_record.get_multiple_tag_subfield_values(
        [("245", "a"), ("994", "ab")],
        concat=True,
        separator="/",
    ) == ["Bahrain", "02/MYG"]
