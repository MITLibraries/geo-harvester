# ruff: noqa: N802, SLF001

import json

import pytest


def test_aardvark_required_dct_accessRights_s(aardvark_all_fields):
    assert aardvark_all_fields._dct_accessRights_s() == "Restricted"


def test_aardvark_required_dct_title_s(aardvark_all_fields):
    assert aardvark_all_fields._dct_title_s() == "Egypt, Cairo (Topographic Map, 1972)"


def test_aardvark_required_gbl_resourceClass_sm(aardvark_all_fields):
    assert aardvark_all_fields._gbl_resourceClass_sm() == ["Imagery"]


def test_aardvark_required_gbl_resourceClass_sm_umapped_value_gives_other(
    aardvark_all_fields,
):
    aardvark_all_fields._parsed_data = {"nothing": "to see here"}
    assert aardvark_all_fields._gbl_resourceClass_sm() == ["Other"]


def test_aardvark_required_dcat_bbox(aardvark_all_fields):
    assert (
        aardvark_all_fields._dcat_bbox()
        == "ENVELOPE(31.161907, 31.381609, 30.141311, 29.994131)"
    )


def test_aardvark_required_locn_geometry(aardvark_all_fields):
    polygon = (
        "POLYGON ((74.0060 40.7128, 71.0589 42.3601, 73.7562 42.6526, 74.0060 40.7128))"
    )
    aardvark_all_fields._parsed_data = {"locn_geometry": polygon}
    assert aardvark_all_fields._locn_geometry() == polygon


def test_aardvark_required_dct_references_s(aardvark_all_fields):
    assert aardvark_all_fields._dct_references_s() == json.dumps(
        {
            "http://schema.org/url": (
                "https://geodata.libraries.mit.edu/record/gismit:EG_CAIRO_A25TOPO_1972"
            )
        }
    )


def test_aardvark_required_dct_references_s_no_url_raise_error(aardvark_all_fields):
    aardvark_all_fields._parsed_data = {"dct_references_s": "{}"}
    with pytest.raises(
        ValueError, match="Could not determine external URL from source metadata"
    ):
        aardvark_all_fields._dct_references_s()


def test_aardvark_required_dct_references_s_includes_download_url(aardvark_all_fields):
    aardvark_all_fields._parsed_data = {
        "dct_references_s": json.dumps(
            {
                "http://schema.org/url": "http://example.com/abc213",
                "http://schema.org/downloadUrl": "http://example.com/abc213.zip",
                "http://schema.org/notUsed": "http://example.com/something/else",
            }
        )
    }
    assert aardvark_all_fields._dct_references_s() == json.dumps(
        {
            "http://schema.org/url": "http://example.com/abc213",
            "http://schema.org/downloadUrl": [
                {
                    "label": "Data",
                    "url": "http://example.com/abc213.zip",
                }
            ],
        }
    )


def test_aardvark_dct_description_sm(aardvark_all_fields):
    assert aardvark_all_fields._dct_description_sm() == [
        "This layer is a georeferenced raster image of a paper map entitled Cairo ("
        "H-36-63, 75). The original scanned image with the legend and other information "
        "regarding the source is available in Dome. See the online linkage."
    ]


def test_aardvark_dcat_keyword_sm(aardvark_all_fields):
    assert aardvark_all_fields._dcat_keyword_sm() == ["fish", "snails"]


def test_aardvark_dct_alternative_sm(aardvark_all_fields):
    assert aardvark_all_fields._dct_alternative_sm() == ["This is another title"]


def test_aardvark_dct_creator_sm(aardvark_all_fields):
    assert aardvark_all_fields._dct_creator_sm() == [
        "Soviet Union. Sovetskaia Armiia. Generalnyi Shtab (Soviet)"
    ]


def test_aardvark_dct_format_s(aardvark_all_fields):
    assert aardvark_all_fields._dct_format_s() == "Shapefile"


def test_aardvark_dct_issued_s(aardvark_all_fields):
    assert aardvark_all_fields._dct_issued_s() == "1972-01-01"


def test_aardvark_dct_identifier_sm(aardvark_all_fields):
    assert aardvark_all_fields._dct_identifier_sm() == [
        "EG_CAIRO_A25TOPO_1972",
        "http://hdl.handle.net/1721.3/172443",
        "EG_CAIRO_A25TOPO_1972.tif",
    ]


def test_aardvark_dct_language_sm(aardvark_all_fields):
    assert aardvark_all_fields._dct_language_sm() == ["eng"]


def test_aardvark_dct_publisher_sm(aardvark_all_fields):
    assert aardvark_all_fields._dct_publisher_sm() == ["LAND INFO Worldwide Mapping, LLC"]


def test_aardvark_dct_rights_sm(aardvark_all_fields):
    assert aardvark_all_fields._dct_rights_sm() == [
        "All data is the copyrighted property of LAND INFO Worldwide Mapping, "
        "LLC and/or its suppliers. Land Info grants customer unlimited, perpetual "
        "license for academic use of data and license to distribute data on a "
        "non-commercial basis, or as Derived Works. Derived works that include the "
        "source data must be merged with other value-added data in such a way that the "
        "derived work can\u2019t be converted back to the original source data format. "
        "Other Derived Works that don\u2019t include the source data (vector "
        "extraction, classification etc.) have no restrictions on use and distribution. "
        "An unlimited license is granted for media use, provided that the following "
        'citation is used: "map data courtesy www.landinfo.com."'
    ]


def test_aardvark_dct_spatial_sm(aardvark_all_fields):
    assert aardvark_all_fields._dct_spatial_sm() == ["Egypt", "Cairo"]


def test_aardvark_dct_subject_sm(aardvark_all_fields):
    assert aardvark_all_fields._dct_subject_sm() == [
        "maps",
        "topographic maps",
        "raster",
        "land use",
        "imageryBaseMapsEarthCover",
        "elevation",
    ]


def test_aardvark_dct_temporal_sm(aardvark_all_fields):
    assert aardvark_all_fields._dct_temporal_sm() == ["1972-01-01"]


def test_aardvark_gbl_dateRange_drsim(aardvark_all_fields):
    assert aardvark_all_fields._gbl_dateRange_drsim() == ["[1990 TO 1991]"]


def test_aardvark_gbl_dateRange_drsim_poorly_formed_string_to_list(aardvark_all_fields):
    # NOTE: 'gbl_dateRange_drsim' should be list value, but some OGM records have scalar
    aardvark_all_fields._parsed_data = {"gbl_dateRange_drsim": "[1990 TO 1991]"}
    assert aardvark_all_fields._gbl_dateRange_drsim() == ["[1990 TO 1991]"]


def test_aardvark_gbl_resourceType_sm(aardvark_all_fields):
    assert aardvark_all_fields._gbl_resourceType_sm() == ["Raster data"]


def test_aardvark_gbl_indexYear_im(aardvark_all_fields):
    assert aardvark_all_fields._gbl_indexYear_im() == [1972]
