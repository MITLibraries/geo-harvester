"""tests.test_records.test_marc"""

# ruff: noqa: N802, SLF001

from decimal import Decimal

from harvester.records.formats.marc import MARC

BAD_COORDINATE_STRING = "X999"


def test_marc_helper_pad_coordinate_string():
    assert (
        MARC._bbox_pad_coordinate_string(BAD_COORDINATE_STRING) == BAD_COORDINATE_STRING
    )
    assert MARC._bbox_pad_coordinate_string("E123") == "E0000123"
    assert MARC._bbox_pad_coordinate_string("E1234567") == "E1234567"


def test_marc_helper_convert_coordinate_string_to_decimal():
    assert MARC._bbox_convert_coordinate_string_to_decimal("E0503300") == Decimal("50.55")
    assert MARC._bbox_convert_coordinate_string_to_decimal("W0503300") == Decimal(
        "-50.55"
    )
    assert MARC._bbox_convert_coordinate_string_to_decimal("N0260139") == Decimal(
        "26.02750000"
    )
    assert MARC._bbox_convert_coordinate_string_to_decimal(BAD_COORDINATE_STRING) is None


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
    assert almamarc_source_record._gbl_resourceClass_sm() == ["Imagery"]


def test_marc_record_required_dcat_bbox(almamarc_source_record):
    assert (
        almamarc_source_record._dcat_bbox()
        == "ENVELOPE(50.55, 50.55, 26.02750000, 26.02750000)"
    )


def test_marc_record_required_dcat_bbox_missing_034(almamarc_source_record_missing_034):
    assert almamarc_source_record_missing_034._dcat_bbox() is None


def test_marc_record_required_dcat_bbox_multiple_034(almamarc_source_record_multiple_034):
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
    almamarc_source_record_missing_034,
):
    assert almamarc_source_record_missing_034._locn_geometry() is None


#################################
# Optional Fields
#################################


#################################
# Helpers
#################################
def test_marc_record_get_bounding_box_missing_subfield_return_none(
    almamarc_source_record_missing_subfield_034,
):
    assert almamarc_source_record_missing_subfield_034.get_largest_bounding_box() is None


def test_marc_record_get_bounding_box_invalid_subfield_return_none(
    almamarc_source_record_invalid_subfield_034,
):
    assert almamarc_source_record_invalid_subfield_034.get_largest_bounding_box() is None


def test_marc_record_bbox_extract_data_from_tags_missing_subfield_less_data(
    almamarc_source_record_missing_subfield_034,
):
    tags = almamarc_source_record_missing_subfield_034.marc.field("034")
    data = almamarc_source_record_missing_subfield_034._bbox_extract_data_from_tags(tags)
    assert data == {
        "w": [Decimal("50.55")],
        "e": [Decimal("50.55")],
        "n": [Decimal("26.02750000")],
        # note missing "s" direction value from missing subfield
    }
