"""tests.test_records.test_marc"""

# ruff: noqa: N802, SLF001

from decimal import Decimal

import marcalyx
from lxml import etree

from harvester.records.formats.marc import MARC

BAD_COORDINATE_STRING = "X999"


def add_new_datafield(
    source_record: MARC,
    tag,
    ind1=" ",
    ind2=" ",
    subfields=None,
):
    datafield = etree.SubElement(source_record.marc.node, "datafield")
    datafield.set("tag", tag)
    datafield.set("ind1", ind1)
    datafield.set("ind2", ind2)
    if subfields is not None:
        for code, text in subfields:
            subfield = etree.SubElement(datafield, "subfield")
            subfield.set("code", code)
            subfield.text = text
    source_record.marc = marcalyx.Record(source_record.marc.node)


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


def test_marc_record_required_gbl_resourceClass_sm_multiple(almamarc_source_record):
    add_new_datafield(
        almamarc_source_record,
        "336",
        subfields=[("a", "cartographic images")],
    )
    assert almamarc_source_record._gbl_resourceClass_sm() == ["Imagery", "Maps"]


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


def test_marc_record_required_dct_description_sm(almamarc_source_record):
    add_new_datafield(
        almamarc_source_record,
        "520",
        subfields=[("a", "I am a detailed description")],
    )
    assert almamarc_source_record._dct_description_sm() == ["I am a detailed description"]


def test_marc_record_required_dct_alternative_sm(almamarc_source_record):
    add_new_datafield(
        almamarc_source_record,
        "246",
        subfields=[("a", "Alt Title Here")],
    )
    add_new_datafield(
        almamarc_source_record,
        "730",
        subfields=[("a", "Alt Title Here 2")],
    )
    assert set(almamarc_source_record._dct_alternative_sm()) == {
        "Alt Title Here",
        "Alt Title Here 2",
    }


def test_marc_record_required_dct_creator_sm(almamarc_source_record):
    assert set(almamarc_source_record._dct_creator_sm()) == {
        "Fairey Surveys Ltd.",
        "Falcon Publishing.",
        "Parrish Rogers International Ltd.",
    }


def test_marc_record_required_dct_format_s(almamarc_source_record):
    # dct_format_s is defined for documentation, but always returns None
    assert almamarc_source_record._dct_format_s() is None


def test_marc_record_required_dct_publisher_sm(almamarc_source_record):
    assert set(almamarc_source_record._dct_publisher_sm()) == {
        "Fairey",
    }


def test_marc_record_required_gbl_resourceType_sm(almamarc_source_record):
    assert set(almamarc_source_record._gbl_resourceType_sm()) == {
        "Road maps",
        "Tourist maps",
    }


def test_marc_record_required_dct_issued_s(almamarc_source_record):
    assert almamarc_source_record._dct_issued_s() == "1979"


def test_marc_record_required_dct_identifier_sm(almamarc_source_record):
    # set fixture identifier to real identifier
    almamarc_source_record.identifier = almamarc_source_record.get_identifier_from_001(
        almamarc_source_record.marc
    )
    assert set(almamarc_source_record._dct_identifier_sm()) == {
        "990022897960106761",
        "80692167",
        "0906358019",
        "9780906358016",
        "(MCM)002289796MIT01",
        "(OCoLC)06533196",
    }


def test_marc_record_required_dct_temporal_sm(almamarc_source_record):
    add_new_datafield(
        almamarc_source_record,
        "245",
        subfields=[("f", "circa. 1991")],
    )
    add_new_datafield(
        almamarc_source_record,
        "651",
        subfields=[("y", "1990-1991")],
    )
    assert set(almamarc_source_record._dct_temporal_sm()) == {
        "1979",
        "1990-1991",
        "circa. 1991",
    }


def test_marc_record_required_gbl_dateRange_drsim(almamarc_source_record):
    add_new_datafield(
        almamarc_source_record,
        "245",
        subfields=[("f", "circa. 1991")],
    )
    add_new_datafield(
        almamarc_source_record,
        "651",
        subfields=[("y", "1990-1991")],
    )
    assert set(almamarc_source_record._gbl_dateRange_drsim()) == {"[1990 TO 1991]"}


def test_marc_record_required_gbl_indexYear_im(almamarc_source_record):
    add_new_datafield(
        almamarc_source_record,
        "245",
        subfields=[("f", "circa. 1991")],
    )
    add_new_datafield(
        almamarc_source_record,
        "651",
        subfields=[("y", "1990-1991")],
    )
    assert set(almamarc_source_record._gbl_indexYear_im()) == {1979, 1990, 1991}


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
