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


def test_iso19139_record_required_dcat_bbox(iso19139_source_record_required_fields):
    assert (
        iso19139_source_record_required_fields._dcat_bbox()
        == "ENVELOPE(88, -16.5, 138, 25.833333)"
    )


def test_iso19139_record_required_locn_geometry(iso19139_source_record_required_fields):
    assert (
        iso19139_source_record_required_fields._locn_geometry()
        == "ENVELOPE(88, -16.5, 138, 25.833333)"
    )


    )


def test_iso19139_field_dct_title_s_none_returned(valid_mit_iso19139_source_record):
    with patch.object(XMLSourceRecord, "xpath_query") as mocked_xpath:
        mocked_xpath.return_value = []
        assert valid_mit_iso19139_source_record._dct_title_s() is None
