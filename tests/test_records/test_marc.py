"""tests.test_records.test_marc"""

# ruff: noqa: N802, SLF001

from decimal import Decimal

from harvester.records.formats.marc import MARC

BAD_COORDINATE_STRING = "X999"


def test_marc_helper_pad_coordinate_string():
    assert MARC.pad_coordinate_string(BAD_COORDINATE_STRING) == BAD_COORDINATE_STRING
    assert MARC.pad_coordinate_string("E123") == "E0000123"
    assert MARC.pad_coordinate_string("E1234567") == "E1234567"


def test_marc_helper_convert_coordinate_string_to_decimal():
    assert MARC.convert_coordinate_string_to_decimal("E0503300") == Decimal("50.55")
    assert MARC.convert_coordinate_string_to_decimal("W0503300") == Decimal("-50.55")
    assert MARC.convert_coordinate_string_to_decimal("N0260139") == Decimal("26.02750000")
    assert MARC.convert_coordinate_string_to_decimal(BAD_COORDINATE_STRING) is None


#################################
# Required Fields
#################################


def test_marc_record_required_dct_accessRights_s(almamarc_source_record):
    assert almamarc_source_record._dct_accessRights_s() == "Public"


def test_marc_record_required_dct_title_s(almamarc_source_record):
    assert (
        almamarc_source_record._dct_title_s()
        == "Bahrain [cartographic material] / map & town plans produced by Fairey "
        "Surveys Limited."
    )


def test_marc_record_required_gbl_resourceClass_sm(almamarc_source_record):
    assert almamarc_source_record._gbl_resourceClass_sm() == ["Maps"]


def test_marc_record_required_dcat_bbox(almamarc_source_record):
    assert (
        almamarc_source_record._dcat_bbox()
        == "ENVELOPE(50.55, 50.55, 26.02750000, 26.02750000)"
    )


def test_marc_record_required_dcat_bbox_missing_034(
    caplog, almamarc_source_record_missing_034
):
    caplog.set_level("DEBUG")
    assert almamarc_source_record_missing_034._dcat_bbox() is None
    assert "Record does not have valid 034 tag(s), cannot determine bbox." in caplog.text


def test_marc_record_required_dcat_bbox_multiple_034(
    caplog, almamarc_source_record_multiple_034
):
    caplog.set_level("DEBUG")
    assert (
        almamarc_source_record_multiple_034._dcat_bbox()
        == "ENVELOPE(40.55, 50.55, 26.02750000, 16.02750000)"
    )


def test_marc_record_required_locn_geometry_bbox(almamarc_source_record_multiple_034):
    assert (
        almamarc_source_record_multiple_034._locn_geometry()
        == "ENVELOPE(40.55, 50.55, 26.02750000, 16.02750000)"
    )


def test_marc_record_required_locn_geometry_point(almamarc_source_record):
    assert almamarc_source_record._locn_geometry() == "POINT(50.55, 26.02750000)"


def test_marc_record_required_locn_geometry_missing_034(
    caplog, almamarc_source_record_missing_034
):
    caplog.set_level("DEBUG")
    assert almamarc_source_record_missing_034._locn_geometry() is None
    assert "Record does not have valid 034 tag(s), cannot determine bbox." in caplog.text


#################################
# Optional Fields
#################################


#################################
# Helpers
#################################
def test_marc_record_required_get_bounding_box_missing_subfield_return_none(
    caplog, almamarc_source_record_missing_subfield_034
):
    caplog.set_level("DEBUG")
    assert almamarc_source_record_missing_subfield_034._get_largest_bounding_box() is None
    assert "Record does not have valid 034 tag(s), cannot determine bbox." in caplog.text


def test_marc_record_required_get_bounding_box_invalid_subfield_return_none(
    caplog, almamarc_source_record_invalid_subfield_034
):
    caplog.set_level("DEBUG")
    assert almamarc_source_record_invalid_subfield_034._get_largest_bounding_box() is None
