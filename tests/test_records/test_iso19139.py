# ruff: noqa: N802, SLF001
from unittest.mock import patch

import pytest
from dateutil.parser import ParserError
from lxml.etree import Element

from harvester.records import ISO19139

#################################
# Required Fields
#################################


def test_iso19139_record_required_dct_accessRights_s(
    iso19139_source_record_required_fields,
):
    assert iso19139_source_record_required_fields._dct_accessRights_s() == "Public"


def test_iso19139_dct_accessRights_s_missing_element_default_public(
    iso19139_source_record_required_fields, xpath_returns_nothing
):
    assert iso19139_source_record_required_fields._dct_accessRights_s() == "Public"


def test_iso19139_record_is_restricted_is_true_success(
    iso19139_source_record_required_fields,
):
    assert not iso19139_source_record_required_fields.is_restricted


def test_iso19139_record_required_dct_title_s(iso19139_source_record_required_fields):
    assert (
        iso19139_source_record_required_fields._dct_title_s()
        == "Nihyakumanbunnoichi Nanpōzu, Maps Index"
    )


def test_iso19139_record_required_dct_title_s_missing_raises_error(
    iso19139_source_record_required_fields, xpath_returns_nothing
):
    with pytest.raises(ValueError, match="Could not find <title> element"):
        iso19139_source_record_required_fields._dct_title_s()


def test_iso19139_record_required_gbl_resourceClass_sm(
    iso19139_source_record_required_fields,
):
    assert iso19139_source_record_required_fields._gbl_resourceClass_sm() == ["Datasets"]


def test_iso19139_record_required_gbl_resourceClass_sm_missing_return_empty_list(
    iso19139_source_record_required_fields, xpath_returns_nothing
):
    assert iso19139_source_record_required_fields._gbl_resourceClass_sm() == []


def test_iso19139_record_required_gbl_resourceClass_sm_unhandled_value_return_none(
    iso19139_source_record_required_fields, strings_from_xpath_unhandled_value
):
    assert iso19139_source_record_required_fields._gbl_resourceClass_sm() == []


#################################
# Optional Fields
#################################


def test_iso19139_record_required_dcat_bbox(iso19139_source_record_all_fields):
    assert (
        iso19139_source_record_all_fields._dcat_bbox()
        == "ENVELOPE(88, 138, 25.833333, -16.5)"
    )


def test_iso19139_optional_dcat_keyword_sm(iso19139_source_record_all_fields):
    assert iso19139_source_record_all_fields._dcat_keyword_sm() == [
        "Southeast Asia",
        "polygon",
        "Grids (Cartography)",
        "Index maps",
        "Military maps",
        "Topographic maps",
        "Downloadable Data",
    ]


def test_iso19139_optional_dct_alternative_sm(iso19139_source_record_all_fields):
    assert iso19139_source_record_all_fields._dct_alternative_sm() == [
        "Southeast Asia, 1:2,000,000 Maps Index"
    ]


def test_iso19139_optional_dct_identifier_sm(iso19139_source_record_all_fields):
    assert iso19139_source_record_all_fields._dct_identifier_sm() == [
        "http://purl.stanford.edu/yp709vs3743"
    ]


def test_iso19139_optional_dct_subject_sm(iso19139_source_record_all_fields):
    assert iso19139_source_record_all_fields._dct_subject_sm() == [
        "boundaries",
        "intelligenceMilitary",
    ]


def test_iso19139_optional_dcat_theme_sm(iso19139_source_record_all_fields):
    assert iso19139_source_record_all_fields._dcat_theme_sm() == [
        "Boundaries",
    ]


def test_iso19139_optional_dct_spatial_sm(iso19139_source_record_all_fields):
    assert iso19139_source_record_all_fields._dct_spatial_sm() == ["Southeast Asia"]


def test_iso19139_optional_dct_temporal_sm(iso19139_source_record_all_fields):
    assert iso19139_source_record_all_fields._dct_temporal_sm() == [
        "1990-11-03",
        "1941-1944",
    ]


def test_iso19139_optional_dct_temporal_sm_bad_date_logs_error_and_continues(
    caplog, iso19139_source_record_all_fields
):
    caplog.set_level("DEBUG")
    with patch(
        "harvester.records.iso19139.date_parser",
        side_effect=ParserError("Bad date here"),
    ):
        assert iso19139_source_record_all_fields._dct_temporal_sm() == []
        assert "Could not extract begin or end date from time period" in caplog.text


def test_iso19139_optional_gbl_dateRange_drsim(iso19139_source_record_all_fields):
    assert iso19139_source_record_all_fields._gbl_dateRange_drsim() == ["1941 TO 1944"]


def test_iso19139_optional_gbl_dateRange_drsim_bad_date_logs_error_and_continues(
    caplog, iso19139_source_record_all_fields
):
    caplog.set_level("DEBUG")
    with patch(
        "harvester.records.iso19139.date_parser",
        side_effect=ParserError("Bad date here"),
    ):
        assert iso19139_source_record_all_fields._gbl_dateRange_drsim() == []
        assert "Could not extract begin or end date from time period" in caplog.text


def test_iso19139_optional_dct_description_sm(iso19139_source_record_all_fields):
    descriptions = iso19139_source_record_all_fields._dct_description_sm()
    assert descriptions[0].startswith(
        "This polygon shapefile is an index to 1:2,000,000 scale maps of Southeast Asia"
    )


def test_iso19139_optional_dct_creator_sm(iso19139_source_record_all_fields):
    assert iso19139_source_record_all_fields._dct_creator_sm() == [
        "Stanford Geospatial Center"
    ]


def test_iso19139_optional_dct_format_s(iso19139_source_record_all_fields):
    assert iso19139_source_record_all_fields._dct_format_s() == "Shapefile"


def test_iso19139_dct_format_s_missing_element_default_restricted(
    iso19139_source_record_required_fields, xpath_returns_nothing
):
    assert iso19139_source_record_required_fields._dct_format_s() is None


def test_iso19139_optional_dct_issued_s(iso19139_source_record_all_fields):
    assert iso19139_source_record_all_fields._dct_issued_s() == "2016-05-01"


def test_iso19139_optional_dct_issued_s_date_parse_error(
    caplog, iso19139_source_record_all_fields
):
    caplog.set_level("DEBUG")
    with patch(
        "harvester.records.iso19139.date_parser",
        side_effect=ParserError("Bad date here"),
    ):
        assert iso19139_source_record_all_fields._dct_issued_s() is None
        assert "Error parsing date string" in caplog.text


def test_iso19139_optional_dct_language_sm(iso19139_source_record_all_fields):
    assert iso19139_source_record_all_fields._dct_language_sm() == ["eng"]


def test_iso19139_optional_dct_language_sm_parse_error_log_continue(
    caplog, iso19139_source_record_all_fields
):
    caplog.set_level("DEBUG")
    with patch(
        "harvester.records.iso19139.convert_lang_code",
        side_effect=Exception("Parsing Error"),
    ):
        assert iso19139_source_record_all_fields._dct_language_sm() == []
        assert "Error parsing language code" in caplog.text


def test_iso19139_optional_dct_publisher_sm(iso19139_source_record_all_fields):
    assert iso19139_source_record_all_fields._dct_publisher_sm() == [
        "Stanford Digital Repository"
    ]


def test_iso19139_optional_dct_rights_sm(iso19139_source_record_all_fields):
    assert iso19139_source_record_all_fields._dct_rights_sm() == [
        "This item is in the public domain. There are no restrictions on access or use."
    ]


def test_iso19139_optional_gbl_indexYear_im(iso19139_source_record_all_fields):
    # Note the repeating years, this is deduped via the calling .normalize() method
    assert set(iso19139_source_record_all_fields._gbl_indexYear_im()) == {
        1941,
        1944,
        1990,
    }


def test_iso19139_optional_gbl_indexYear_im_date_parse_log_continue(
    caplog, iso19139_source_record_all_fields
):
    caplog.set_level("DEBUG")
    with patch.object(
        ISO19139,
        "_get_temporal_extents",
        return_value={
            "instances": [
                {
                    "description": None,
                    "timestamp": "I am a bad date.",
                },
            ],
            "periods": [
                {
                    "description": None,
                    "begin_timestamp": "I am ANOTHER bad date.",
                    "end_timestamp": "I am YET ANOTHER bad date.",
                }
            ],
        },
    ):
        assert iso19139_source_record_all_fields._gbl_indexYear_im() == []
        assert "Could not extract year from date string" in caplog.text


def test_iso19139_optional_gbl_resourceType_sm(iso19139_source_record_all_fields):
    assert iso19139_source_record_all_fields._gbl_resourceType_sm() == ["Polygon data"]


def test_iso19139_record_required_locn_geometry(iso19139_source_record_all_fields):
    assert (
        iso19139_source_record_all_fields._locn_geometry()
        == "ENVELOPE(88, 138, 25.833333, -16.5)"
    )


def test_iso19139_get_temporal_extents_instant_and_periods_success(
    iso19139_source_record_all_fields,
):
    temporal_elements = iso19139_source_record_all_fields._get_temporal_extents()
    assert temporal_elements == {
        "periods": [
            {
                "description": "ground condition",
                "begin_timestamp": "1941-01-01T00:00:00",
                "end_timestamp": "1944-12-31T00:00:00",
            }
        ],
        "instances": [
            {
                "description": "ground condition",
                "timestamp": "1990-11-03T00:00:00",
            }
        ],
    }


def test_iso19139_parse_time_position_value_parsing(iso19139_source_record_all_fields):
    # test None element
    assert iso19139_source_record_all_fields._parse_time_position(None) is None

    # test timestamp from text
    time_position = Element("timePosition")
    time_position.attrib["indeterminatePosition"] = "2023"
    assert iso19139_source_record_all_fields._parse_time_position(time_position) == "2023"

    # test timestamp from text
    time_position = Element("timePosition")
    time_position.text = "2023-10-10"
    assert (
        iso19139_source_record_all_fields._parse_time_position(time_position)
        == "2023-10-10"
    )

    # test empty element
    time_position = Element("timePosition")
    assert iso19139_source_record_all_fields._parse_time_position(time_position) is None
