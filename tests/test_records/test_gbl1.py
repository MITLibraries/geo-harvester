# ruff: noqa: N802, SLF001

import json

import pytest


def test_gbl1_convert_scalar_to_array_no_value_get_list(gbl1_all_fields):
    assert gbl1_all_fields._convert_scalar_to_array("watermelon") == []


def test_gbl1_required_dct_accessRights_s(gbl1_all_fields):
    assert gbl1_all_fields._dct_accessRights_s() == "Public"


def test_gbl1_required_dct_title_s(gbl1_all_fields):
    assert (
        gbl1_all_fields._dct_title_s()
        == "United Arab Emirates (Geographic Feature Names, 2003)"
    )


def test_gbl1_required_gbl_resourceClass_sm(gbl1_all_fields):
    assert gbl1_all_fields._gbl_resourceClass_sm() == ["Datasets"]


def test_gbl1_required_gbl_resourceClass_sm_umapped_value_gives_other(gbl1_all_fields):
    gbl1_all_fields._parsed_data = {"nothing": "to see"}
    assert gbl1_all_fields._gbl_resourceClass_sm() == ["Other"]


def test_gbl1_required_dcat_bbox(gbl1_all_fields):
    assert gbl1_all_fields._dcat_bbox() == "ENVELOPE(45, 59.25, 26.133333, 22.166667)"


def test_gbl1_required_locn_geometry(gbl1_all_fields):
    assert gbl1_all_fields._locn_geometry() == "ENVELOPE(45, 59.25, 26.133333, 22.166667)"


def test_gbl1_required_dct_references_s(gbl1_all_fields):
    assert gbl1_all_fields._dct_references_s() == json.dumps(
        {
            "http://schema.org/url": "https://geodata.libraries.mit.edu/record/abc123",
            "http://schema.org/downloadUrl": [
                {
                    "label": "Data",
                    "url": "https://geodata.libraries.mit.edu/record/abc123.zip",
                }
            ],
        }
    )


def test_gbl1_required_dct_description_sm(gbl1_all_fields):
    assert gbl1_all_fields._dct_description_sm() == [
        "Geographic feature names for United Arab Emirates."
    ]


def test_gbl1_required_dcat_keyword_sm(gbl1_all_fields):
    # NOTE: field not mapped from GBL1
    assert gbl1_all_fields._dcat_keyword_sm() == []


def test_gbl1_required_dct_alternative_sm(gbl1_all_fields):
    # NOTE: field not mapped from GBL1
    assert gbl1_all_fields._dct_alternative_sm() == []


def test_gbl1_required_dct_creator_sm(gbl1_all_fields):
    assert gbl1_all_fields._dct_creator_sm() == ["National Imagery and Mapping Agency"]


def test_gbl1_required_dct_format_s(gbl1_all_fields):
    assert gbl1_all_fields._dct_format_s() == "Shapefile"


def test_gbl1_required_dct_issued_s(gbl1_all_fields):
    assert gbl1_all_fields._dct_issued_s() == "2003-10-01"


def test_gbl1_required_dct_identifier_sm(gbl1_all_fields):
    assert gbl1_all_fields._dct_identifier_sm() == [
        "http://example.com/IAmUniqueId123",
    ]


def test_gbl1_required_dct_language_sm(gbl1_all_fields):
    assert gbl1_all_fields._dct_language_sm() == [
        "English",
    ]


def test_gbl1_required_dct_language_sm_handle_poorly_formed_but_encountered_field_name(
    gbl1_all_fields,
):
    # NOTE: 'dc_language_sm' is not a valid GBL1 field, but present in many OGM records
    gbl1_all_fields._parsed_data = {"dc_language_sm": "English"}
    assert gbl1_all_fields._dct_language_sm() == [
        "English",
    ]


def test_gbl1_required_dct_language_sm_missing_returns_empty_list(
    gbl1_all_fields,
):
    gbl1_all_fields._parsed_data = {"nothing": "to see here"}
    assert gbl1_all_fields._dct_language_sm() == []


def test_gbl1_required_dct_publisher_sm(gbl1_all_fields):
    assert gbl1_all_fields._dct_publisher_sm() == [
        "U.S. National Imagery and Mapping Agency (NIMA)",
    ]


def test_gbl1_required_dct_rights_sm(gbl1_all_fields):
    # NOTE: field not mapped from GBL1
    assert gbl1_all_fields._dct_rights_sm() == []


def test_gbl1_required_dct_spatial_sm(gbl1_all_fields):
    assert gbl1_all_fields._dct_spatial_sm() == ["United Arab Emirates"]


def test_gbl1_required_dct_subject_sm(gbl1_all_fields):
    assert gbl1_all_fields._dct_subject_sm() == ["locations", "Names, Geographical"]


def test_gbl1_required_dct_temporal_sm(gbl1_all_fields):
    assert gbl1_all_fields._dct_temporal_sm() == ["2003"]


def test_gbl1_required_gbl_dateRange_drsim(gbl1_all_fields):
    # NOTE: field not mapped from GBL1
    assert gbl1_all_fields._gbl_dateRange_drsim() == []


def test_gbl1_required_gbl_resourceType_sm(gbl1_all_fields):
    assert gbl1_all_fields._gbl_resourceType_sm() == ["Polygon data"]


def test_gbl1_required_gbl_indexYear_im(gbl1_all_fields):
    assert gbl1_all_fields._gbl_indexYear_im() == [2003]


def test_gbl1_required_gbl_indexYear_im_poorly_formed_but_encountered_array(
    gbl1_all_fields,
):
    # NOTE: 'solr_year_i' should be scalar value, but many OGM records have array
    gbl1_all_fields._parsed_data = {"solr_year_i": [2003]}
    assert gbl1_all_fields._gbl_indexYear_im() == [2003]


def test_gbl1_required_gbl_indexYear_im_missing_value_return_list(gbl1_all_fields):
    gbl1_all_fields._parsed_data = {"nothing": "to see here"}
    assert gbl1_all_fields._gbl_indexYear_im() == []


def test_gbl1_alternate_url_strategy_base_url_and_slug(gbl1_all_fields):
    gbl1_all_fields.ogm_repo_config["external_url_strategy"] = {
        "name": "base_url_and_slug",
        "base_url": "http://example.com",
        "gbl1_field": "layer_slug_s",
    }
    links = json.loads(gbl1_all_fields._dct_references_s())
    assert (
        links["http://schema.org/url"] == "http://example.com/MIT-SDE_DATA.AE_A8GNS_2003"
    )


def test_gbl1_alternate_url_strategy_field_value(gbl1_all_fields):
    gbl1_all_fields.ogm_repo_config["external_url_strategy"] = {
        "name": "field_value",
        "gbl1_field": "dc_identifier_s",
    }
    links = json.loads(gbl1_all_fields._dct_references_s())
    assert links["http://schema.org/url"] == "http://example.com/IAmUniqueId123"


def test_gbl1_alternate_url_strategy_field_value_non_url_return_none(gbl1_all_fields):
    gbl1_all_fields.ogm_repo_config["external_url_strategy"] = {
        "name": "field_value",
        "gbl1_field": "layer_slug_s",
    }
    with pytest.raises(
        ValueError, match="Could not determine external URL from source metadata"
    ):
        json.loads(gbl1_all_fields._dct_references_s())


def test_gbl1_alternate_url_strategy_not_recognized_raise_error(gbl1_all_fields):
    gbl1_all_fields.ogm_repo_config["external_url_strategy"] = {
        "name": "bad_strategy_here"
    }
    with pytest.raises(
        ValueError, match="Alternate URL strategy not recognized: bad_strategy_here"
    ):
        json.loads(gbl1_all_fields._dct_references_s())
