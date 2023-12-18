# ruff: noqa: N802, SLF001
from unittest.mock import patch

import pytest
from dateutil.parser import ParserError

from harvester.records import FGDC

#################################
# Required Fields
#################################


def test_fgdc_record_required_dct_accessRights_s(fgdc_source_record_required_fields):
    assert fgdc_source_record_required_fields._dct_accessRights_s() == "Restricted"


def test_fgdc_dct_accessRights_s_missing_element_default_restricted(
    fgdc_source_record_required_fields, xpath_returns_nothing
):
    assert fgdc_source_record_required_fields._dct_accessRights_s() == "Restricted"


def test_fgdc_record_is_restricted_is_true_success(
    fgdc_source_record_required_fields,
):
    assert fgdc_source_record_required_fields.is_restricted


def test_fgdc_record_required_dct_title_s(fgdc_source_record_required_fields):
    assert (
        fgdc_source_record_required_fields._dct_title_s()
        == "Egypt, Cairo (Topographic Map, 1972)"
    )


def test_fgdc_record_required_dct_title_s_missing_raises_error(
    fgdc_source_record_required_fields, xpath_returns_nothing
):
    with pytest.raises(ValueError, match="Could not find <title> element"):
        fgdc_source_record_required_fields._dct_title_s()


def test_fgdc_record_required_gbl_resourceClass_sm(fgdc_source_record_required_fields):
    assert fgdc_source_record_required_fields._gbl_resourceClass_sm() == ["Image"]


def test_fgdc_record_required_gbl_resourceClass_sm_missing_return_empty_list(
    fgdc_source_record_required_fields, xpath_returns_nothing
):
    assert fgdc_source_record_required_fields._gbl_resourceClass_sm() == []


def test_fgdc_record_required_gbl_resourceClass_sm_unhandled_value_return_none(
    fgdc_source_record_required_fields, strings_from_xpath_unhandled_value
):
    assert fgdc_source_record_required_fields._gbl_resourceClass_sm() == []


def test_fgdc_record_required_dcat_bbox(fgdc_source_record_required_fields):
    assert (
        fgdc_source_record_required_fields._dcat_bbox()
        == "ENVELOPE(31.161907, 29.994131, 31.381609, 30.141311)"
    )


def test_fgdc_record_required_locn_geometry(fgdc_source_record_required_fields):
    assert (
        fgdc_source_record_required_fields._locn_geometry()
        == "ENVELOPE(31.161907, 29.994131, 31.381609, 30.141311)"
    )


#################################
# Optional Fields
#################################


def test_fgdc_optional_dct_identifier_sm(fgdc_source_record_all_fields):
    assert fgdc_source_record_all_fields._dct_identifier_sm() == [
        "BKMapPLUTO",
        "US_NY_NYC_BK_G47TXLOTS_2012",
    ]


def test_fgdc_optional_dct_subject_sm(fgdc_source_record_all_fields):
    assert fgdc_source_record_all_fields._dct_subject_sm() == [
        "Land value taxation",
        "City planning",
        "planningCadastre",
        "boundaries",
    ]


def test_fgdc_optional_dcat_theme_sm(fgdc_source_record_all_fields):
    assert fgdc_source_record_all_fields._dcat_theme_sm() == [
        "Boundaries",
    ]


def test_fgdc_optional_dct_spatial_sm(fgdc_source_record_all_fields):
    assert fgdc_source_record_all_fields._dct_spatial_sm() == [
        "New York (State)--New York--Brooklyn"
    ]


def test_fgdc_optional_dct_temporal_sm(fgdc_source_record_all_fields):
    assert fgdc_source_record_all_fields._dct_temporal_sm() == [
        "2012-05-01",
        "2011-05-01",
    ]


def test_fgdc_optional_dct_temporal_sm_bad_date_logs_error_and_continues(
    caplog, fgdc_source_record_all_fields
):
    caplog.set_level("DEBUG")
    with patch(
        "harvester.records.fgdc.date_parser",
        side_effect=ParserError("Bad date here"),
    ):
        assert fgdc_source_record_all_fields._dct_temporal_sm() == []
        assert "Could not parse date string" in caplog.text


def test_fgdc_optional_gbl_dateRange_drsim(fgdc_source_record_all_fields):
    assert fgdc_source_record_all_fields._gbl_dateRange_drsim() == ["[2011 TO 2012]"]


def test_fgdc_optional_gbl_dateRange_drsim_bad_date_logs_error_and_continues(
    caplog, fgdc_source_record_all_fields
):
    caplog.set_level("DEBUG")
    with patch(
        "harvester.records.fgdc.date_parser",
        side_effect=ParserError("Bad date here"),
    ):
        assert fgdc_source_record_all_fields._gbl_dateRange_drsim() == []
        assert "Could not extract begin or end date from date range" in caplog.text


def test_fgdc_optional_dct_description_sm(fgdc_source_record_all_fields):
    assert fgdc_source_record_all_fields._dct_description_sm() == [
        "This dataset represents a compilation of data from various government agencies "
        "throughout the City of New York. The underlying geography is derived from the "
        "Tax Lot Polygon feature class which is part of the Department of Finance's "
        "Digital Tax Map (DTM). The tax lots have been clipped to the shoreline, "
        "as defined by NYCMap planimetric features. The attribute information is from "
        "the Department of City Planning's PLUTO data. The attribute data pertains to "
        "tax lot and building characteristics and geographic, political and "
        "administrative information for each tax lot in New York City. The Tax Lot "
        "Polygon feature class and PLUTO are derived from different sources. As a "
        "result, some PLUTO records did not have a corresponding tax lot in the Tax Lot "
        "polygon feature class at the time of release. These records are included in a "
        "separate non-geographic PLUTO Only DBase (*.dbf) table. There are a number of "
        "reasons why there can be a tax lot in PLUTO that does not match the DTM; the "
        "most common reason is that the various source files are maintained by "
        "different departments and divisions with varying update cycles and criteria "
        "for adding and removing records. The attribute definitions for the PLUTO Only "
        "table are the same as those for MapPLUTO.DCP Mapping Lots includes some "
        "features that are not on the tax maps. They have been added by DCP for "
        "cartographic purposes. They include street center 'malls', traffic islands and "
        "some built streets through parks. These features have very few associated "
        "attributes.DOF - RPAD Data - 3/03/2012DCAS - IPIS Data - 2/29/2012 DCP - "
        "Zoning Data - 3/08/2012 DOF - Major Property Data - 5/01/2011 DCP - E - "
        "Designations - 11/05/2011 LPC - Landmark Data - 2/01/2012 DOF - Digital Tax "
        "Map Data - 3/02/2012 DOF - Mass Appraisal Data - 3/15/2012 DCP - Political and "
        "Administrative District Data - 2/01/2012"
    ]


def test_fgdc_optional_dct_creator_sm(fgdc_source_record_all_fields):
    assert fgdc_source_record_all_fields._dct_creator_sm() == [
        "New York (N.Y.). Department of City Planning"
    ]


def test_fgdc_optional_dct_format_s(fgdc_source_record_all_fields):
    assert fgdc_source_record_all_fields._dct_format_s() == "Vector"


def test_fgdc_dct_format_s_missing_element_default_restricted(
    fgdc_source_record_required_fields, xpath_returns_nothing
):
    assert fgdc_source_record_required_fields._dct_format_s() is None


def test_fgdc_optional_dct_issued_s(fgdc_source_record_all_fields):
    assert fgdc_source_record_all_fields._dct_issued_s() == "2012-05-01"


def test_fgdc_optional_dct_issued_s_date_parse_error(
    caplog, fgdc_source_record_all_fields
):
    caplog.set_level("DEBUG")
    with patch(
        "harvester.records.fgdc.date_parser",
        side_effect=ParserError("Bad date here"),
    ):
        assert fgdc_source_record_all_fields._dct_issued_s() is None
        assert "Error parsing date string" in caplog.text


def test_fgdc_optional_dct_language_sm(fgdc_source_record_all_fields):
    assert fgdc_source_record_all_fields._dct_language_sm() == ["eng"]


def test_fgdc_optional_dct_language_sm_parse_error_log_continue(
    caplog, fgdc_source_record_all_fields
):
    caplog.set_level("DEBUG")
    with patch(
        "harvester.records.fgdc.convert_lang_code",
        side_effect=Exception("Parsing Error"),
    ):
        assert fgdc_source_record_all_fields._dct_language_sm() == []
        assert "Error parsing language code" in caplog.text


def test_fgdc_optional_dct_publisher_sm(fgdc_source_record_all_fields):
    assert fgdc_source_record_all_fields._dct_publisher_sm() == [
        "New York (N.Y.). Department of City Planning"
    ]


def test_fgdc_optional_dct_rights_sm(fgdc_source_record_all_fields):
    assert fgdc_source_record_all_fields._dct_rights_sm() == [
        "The information contained in these files was initially compiled for "
        "governmental use by the New York City Department of City Planning. The "
        "Department and City make no representation as to the accuracy of the "
        "information or its suitability for any purpose. The Department and City "
        "disclaim any liability for errors that may be contained herein and shall not "
        "be responsible for any damages, consequential or actual, arising out of or in "
        "connection with the use of the information."
    ]


def test_fgdc_optional_gbl_indexYear_im(fgdc_source_record_all_fields):
    assert fgdc_source_record_all_fields._gbl_indexYear_im() == [2012, 2011]


def test_fgdc_optional_gbl_indexYear_im_date_parse_log_continue(
    caplog, fgdc_source_record_all_fields
):
    caplog.set_level("DEBUG")
    with patch.object(FGDC, "_dct_temporal_sm", return_value=["I am a bad date."]):
        assert fgdc_source_record_all_fields._gbl_indexYear_im() == []
        assert "Could not extract year from date string" in caplog.text


def test_fgdc_optional_gbl_resourceType_sm(fgdc_source_record_all_fields):
    assert fgdc_source_record_all_fields._gbl_resourceType_sm() == ["G-polygon"]
