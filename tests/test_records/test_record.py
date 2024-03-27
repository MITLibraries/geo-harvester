# ruff: noqa: SLF001, PLR2004, N802, FLY002, D212, D205

import json
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from lxml import etree

from harvester.records import JSONSourceRecord, MITAardvark
from harvester.records.exceptions import FieldMethodError, JSONSchemaValidationError


def test_source_record_data_bytes(valid_generic_xml_source_record):
    assert isinstance(valid_generic_xml_source_record.data, bytes)


def test_xml_source_record_parse_xml_root_success(valid_generic_xml_source_record):
    assert isinstance(valid_generic_xml_source_record.root, etree._Element)


def test_xml_source_record_xpath_success(valid_generic_xml_source_record):
    assert len(valid_generic_xml_source_record.xpath_query("//plants:apple")) == 3


def test_xml_source_record_xpath_bad_namespace_raise_error(
    valid_generic_xml_source_record,
):
    with pytest.raises(etree.XPathEvalError):
        assert len(valid_generic_xml_source_record.xpath_query("//fruit:apple")) == 3


def test_xml_source_record_xpath_no_namespace_zero_results_success(
    valid_generic_xml_source_record,
):
    assert len(valid_generic_xml_source_record.xpath_query("//apple")) == 0


def test_xml_source_record_xpath_syntax_variations_valid(valid_generic_xml_source_record):
    single_line_expression = "//plants:fruits/plants:apples/plants:apple/plants:color"
    multi_line_expression = """
    //plants:fruits
        /plants:apples
            /plants:apple
                /plants:color
    """
    assert valid_generic_xml_source_record.xpath_query(
        single_line_expression
    ) == valid_generic_xml_source_record.xpath_query(multi_line_expression)


def test_xml_source_record_string_list_from_xpath_success(
    valid_generic_xml_source_record,
):
    assert set(
        valid_generic_xml_source_record.string_list_from_xpath("//plants:name")
    ) == {"Delicious, Yellow", "Delicious, Red", "Pink Lady"}
    assert set(
        valid_generic_xml_source_record.string_list_from_xpath("//plants:color")
    ) == {
        "yellow",
        "red",
    }
    assert valid_generic_xml_source_record.string_list_from_xpath("//miss") == []
    assert valid_generic_xml_source_record.string_list_from_xpath(
        "//plants:introduced"
    ) == ["1973"]


def test_xml_source_record_remove_whitespace_success(valid_generic_xml_source_record):
    output_string = valid_generic_xml_source_record.remove_whitespace(
        "Hey there\nthis is a      very messy text\t\t from a lengthy\n\n\nXML file.  "
    )
    assert output_string == "Hey there this is a very messy text from a lengthy XML file."
    assert valid_generic_xml_source_record.remove_whitespace(None) is None
    assert valid_generic_xml_source_record.remove_whitespace("") is None
    assert valid_generic_xml_source_record.remove_whitespace("   ") is None


def test_source_record_normalize_two_fields_success(
    mocked_required_fields_source_record,
):
    normalized_record = mocked_required_fields_source_record.normalize()
    assert normalized_record.dct_title_s == "Pink Lady"
    assert normalized_record.gbl_resourceClass_sm == ["Datasets", "Maps"]


def test_source_record_normalize_field_method_fails_raise_error(
    caplog,
    mocked_required_fields_source_record,
):
    caplog.set_level("DEBUG")
    with patch.object(
        mocked_required_fields_source_record, "_dct_title_s"
    ) as mocked_field_method:
        field_method_exception_message = "CRITICAL ERROR DURING FIELD METHOD"
        mocked_field_method.side_effect = ValueError(field_method_exception_message)
        with pytest.raises(
            FieldMethodError, match="Error getting value for field 'dct_title_s'"
        ):
            mocked_required_fields_source_record.normalize()
        assert field_method_exception_message in caplog.text


def test_mitaardvark_to_dict_success(
    valid_mitaardvark_record_required_fields, valid_mitaardvark_data_required_fields
):
    assert (
        valid_mitaardvark_record_required_fields.to_dict()
        == valid_mitaardvark_data_required_fields
    )


def test_mitaardvark_to_json_success(
    valid_mitaardvark_record_required_fields, valid_mitaardvark_data_required_fields
):
    assert valid_mitaardvark_record_required_fields.to_json() == json.dumps(
        valid_mitaardvark_data_required_fields, indent=2
    )
    assert valid_mitaardvark_record_required_fields.to_json(pretty=False) == json.dumps(
        valid_mitaardvark_data_required_fields
    )


def test_record_output_filename_extension(fgdc_source_record_from_zip):
    assert fgdc_source_record_from_zip.output_filename_extension == "xml"


def test_record_source_output_filenames(fgdc_source_record_from_zip):
    assert (
        fgdc_source_record_from_zip.source_metadata_filename
        == "SDE_DATA_AE_A8GNS_2003.source.fgdc.xml"
    )
    assert (
        fgdc_source_record_from_zip.normalized_metadata_filename
        == "SDE_DATA_AE_A8GNS_2003.normalized.aardvark.json"
    )


def test_record_shared_field_method_id_success(fgdc_source_record_from_zip):
    assert fgdc_source_record_from_zip._id() == "mit:SDE_DATA_AE_A8GNS_2003"


@freeze_time("2024-01-01")
def test_record_shared_field_method_gbl_mdModified_dt_success(
    fgdc_source_record_from_zip,
):
    assert fgdc_source_record_from_zip._gbl_mdModified_dt() == "2024-01-01T00:00:00+00:00"


def test_record_shared_field_method_gbl_mdVersion_s_success(
    fgdc_source_record_from_zip,
):
    assert fgdc_source_record_from_zip._gbl_mdVersion_s() == "Aardvark"


def test_record_shared_field_method_dct_references_s_success(
    fgdc_source_record_from_zip,
):
    references = {
        "http://schema.org/downloadUrl": [
            {
                "label": "Source Metadata",
                "url": "https://cdn.dev1.mitlibrary.net/geo/public"
                "/SDE_DATA_AE_A8GNS_2003.source.fgdc.xml",
            },
            {
                "label": "Aardvark Metadata",
                "url": "https://cdn.dev1.mitlibrary.net/geo/public"
                "/SDE_DATA_AE_A8GNS_2003.normalized.aardvark.json",
            },
            {
                "label": "Data",
                "url": "https://cdn.dev1.mitlibrary.net/geo/public"
                "/SDE_DATA_AE_A8GNS_2003.zip",
            },
        ],
        "http://schema.org/url": (
            "https://geodata.libraries.mit.edu/record/gismit:SDE_DATA_AE_A8GNS_2003"
        ),
    }
    assert fgdc_source_record_from_zip._dct_references_s() == json.dumps(references)


def test_custom_exception_has_original_exception():
    try:
        1 / 0  # noqa: B018
    except Exception as exc:  # noqa: BLE001
        message = "I am the custom exception."
        custom_exception = FieldMethodError(exc, message)
    assert str(custom_exception) == "I am the custom exception."
    assert str(custom_exception.original_exception) == "division by zero"
    assert "1 / 0" in custom_exception.get_formatted_traceback()


def test_record_shared_field_method_schema_provider_s_mit_success(
    fgdc_source_record_from_zip,
):
    assert fgdc_source_record_from_zip._schema_provider_s() == "GIS Lab, MIT Libraries"


def test_xml_source_record_single_string_from_xpath_match_success(
    valid_generic_xml_source_record,
):
    assert (
        valid_generic_xml_source_record.single_string_from_xpath(
            "/plants:fruits/plants:description"
        )
        == "List of fruits."
    )


def test_xml_source_record_single_string_from_xpath_miss_success(
    valid_generic_xml_source_record,
):
    assert (
        valid_generic_xml_source_record.single_string_from_xpath(
            "/plants:fruits/plants:does_not_exist"
        )
        is None
    )


def test_xml_source_record_single_string_from_xpath_multiple_raise_error(
    valid_generic_xml_source_record,
):
    with pytest.raises(ValueError, match="Expected one or none matches for XPath query"):
        valid_generic_xml_source_record.single_string_from_xpath("//plants:description")


def test_mitaardvark_record_required_fields_jsonschema_validation_success(
    caplog, valid_mitaardvark_data_required_fields
):
    caplog.set_level("DEBUG")
    MITAardvark(**valid_mitaardvark_data_required_fields)
    assert "The normalized MITAardvark record is valid" in caplog.text


def test_mitaardvark_record_required_fields_jsonschema_validation_raise_compiled_error(
    caplog, invalid_mitaardvark_data_required_fields
):
    """This test shows the compiled validation errors from JSON schema validation.

    'invalid_mitaardvark_data_required_fields' fixture contains the following schema
    violations:
        1. gbl_mdModified_dt: Date not meeting expected 'date-time' format
        2. gbl_mdVersion_s: Unexpected value provided to a field restricted to a single
           value
        3. gbl_resourceClass_sm: Unexpected value provided to field restricted to a fix
           set of values
        4. dct_accessRights_s: 'None' provided to a required field
        5. id: Integer provided to a field restricted to 'string' values
    """
    caplog.set_level("DEBUG")
    assert invalid_mitaardvark_data_required_fields["dct_accessRights_s"] is None
    with pytest.raises(JSONSchemaValidationError):
        MITAardvark(**invalid_mitaardvark_data_required_fields)
    validation_error_messages = "\n".join(
        [
            "The normalized MITAardvark record is invalid:",
            "field: gbl_mdModified_dt, '2023-12-13' is not a 'date-time'",
            "field: gbl_mdVersion_s, 'Aardvark' was expected",
            (
                "field: gbl_resourceClass_sm[0], 'Invalid' is not one of ['Datasets', "
                "'Maps', 'Imagery', 'Collections', 'Websites', 'Web services', 'Other']"
            ),
            "field: dct_accessRights_s, 'dct_accessRights_s' is a required property",
            "field: id, 1 is not of type 'string'",
        ]
    )
    assert validation_error_messages in caplog.text


def test_mitaardvark_record_optional_fields_jsonschema_validation_success(
    caplog, valid_mitaardvark_data_optional_fields
):
    caplog.set_level("DEBUG")
    MITAardvark(**valid_mitaardvark_data_optional_fields)
    assert "The normalized MITAardvark record is valid" in caplog.text


def test_source_record_is_deleted_property_reads_event(fgdc_source_record_all_fields):
    fgdc_source_record_all_fields.event = "deleted"
    assert fgdc_source_record_all_fields.is_deleted


def test_record_shared_field_method_schema_provider_s_ogm_success(
    gbl1_all_fields,
):
    assert gbl1_all_fields._schema_provider_s() == "Earth"


def test_record_shared_field_dcat_theme_sm_no_subjects_return_empty_list(gbl1_all_fields):
    gbl1_all_fields._parsed_data = {"nothing": "to see here"}
    assert gbl1_all_fields._dcat_theme_sm() == []


def test_record_decode_doubled_encoded_source_json_data_success():
    with open("tests/fixtures/records/double_encoded_json_string_record.json", "rb") as f:
        record = JSONSourceRecord(
            data=f.read(),
            identifier="abc123",
            origin="ogm",
            event="created",
            metadata_format="gbl1",
        )
    assert isinstance(record.parsed_data, dict)
    assert record.parsed_data["dc_title_s"] == "Burundi Administrative Boundaries"


def test_controlled_format_value_direct_match(generic_source_record):
    assert generic_source_record.get_controlled_dct_format_s_term("GeoTIFF") == "GeoTIFF"


def test_controlled_format_value_miss(generic_source_record):
    assert generic_source_record.get_controlled_dct_format_s_term("watermleon") is None


@pytest.mark.parametrize(
    ("raw_value", "controlled_value"),
    [
        ("shp", "Shapefile"),
        ("geotiff", "GeoTIFF"),
        ("tiff", "TIFF"),
        ("jpeg2000", "JPEG2000"),
        ("jpg", "JPEG"),
        ("tiff/jpeg", "Mixed"),
        ("multiple", "Mixed"),
        ("tabular", "Tabular"),
    ],
)
def test_controlled_format_variant_matches(
    generic_source_record, raw_value, controlled_value
):
    assert (
        generic_source_record.get_controlled_dct_format_s_term(raw_value)
        == controlled_value
    )


def test_controlled_format_utilize_gbl_resourceType_sm_for_help_success(
    generic_source_record,
):
    """
    This tests that when SourceRecord._dct_format_s() returns no value, or an unmapped
    value, the method get_controlled_dct_format_s_term() fallsback on values from
    gbl_resourceType_sm() to suggest what the filetype is.
    """
    with patch.object(generic_source_record, "_gbl_resourceType_sm") as x:
        x.return_value = ["Polygon data"]
        assert (
            generic_source_record.get_controlled_dct_format_s_term("watermleon")
            == "Shapefile"
        )


def test_controlled_format_utilize_gbl_resourceType_sm_for_help_miss_return_none(
    generic_source_record,
):
    with patch.object(generic_source_record, "_gbl_resourceType_sm") as x:
        x.return_value = ["I still cannot help"]
        assert (
            generic_source_record.get_controlled_dct_format_s_term("watermleon") is None
        )


def test_controlled_resource_type_value_direct_match(generic_source_record):
    direct_match_terms = ["Cadastral maps", "Raster data", "Multi-spectral data"]
    assert (
        generic_source_record.get_controlled_gbl_resourceType_sm_terms(direct_match_terms)
        == direct_match_terms
    )


def test_controlled_resource_type_value_case_insensitive_and_some_dropped(
    generic_source_record,
):
    assert generic_source_record.get_controlled_gbl_resourceType_sm_terms(
        ["CADASTRAL MAPS", "watermelon"]
    ) == ["Cadastral maps"]


def test_controlled_resource_type_value_miss(generic_source_record):
    assert (
        generic_source_record.get_controlled_gbl_resourceType_sm_terms(
            ["watermelon", "pickles"]
        )
        == []
    )


@pytest.mark.parametrize(
    ("raw_values", "controlled_values"),
    [
        (["G-polygon"], ["Polygon data"]),
        (["Raster"], ["Raster data"]),
        (["point"], ["Point data"]),
        (["line"], ["Line data"]),
        (["Image"], ["Image data"]),
        (["Vector"], ["Vector data"]),
        (["Mixed", "Composite"], ["Mixed"]),  # note: de-duped
    ],
)
def test_controlled_resource_type_variant_matches(
    generic_source_record, raw_values, controlled_values
):
    assert (
        generic_source_record.get_controlled_gbl_resourceType_sm_terms(raw_values)
        == controlled_values
    )


def test_empty_strings_filtered_from_output_aardvark(aardvark_empty_strings):
    assert aardvark_empty_strings.parsed_data["dcat_keyword_sm"] == [
        "",  # note this empty string in original record
        "2022-creator-sprint",
    ]
    normalized_record = aardvark_empty_strings.normalize()
    assert normalized_record.dcat_keyword_sm == ["2022-creator-sprint"]
