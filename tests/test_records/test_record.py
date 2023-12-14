# ruff: noqa: SLF001, PLR2004, N802

import json
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from lxml import etree

from harvester.records.exceptions import FieldMethodError


def test_source_record_data_bytes(valid_generic_xml_source_record):
    assert isinstance(valid_generic_xml_source_record.data, bytes)


def test_xml_source_record_parse_xml_root_success(valid_generic_xml_source_record):
    assert isinstance(valid_generic_xml_source_record.root, etree._Element)


def test_xml_source_record_xpath_success(valid_generic_xml_source_record):
    assert len(valid_generic_xml_source_record.xpath("//plants:apple")) == 3


def test_xml_source_record_xpath_bad_namespace_raise_error(
    valid_generic_xml_source_record,
):
    with pytest.raises(etree.XPathEvalError):
        assert len(valid_generic_xml_source_record.xpath("//fruit:apple")) == 3


def test_xml_source_record_xpath_no_namespace_zero_results_success(
    valid_generic_xml_source_record,
):
    assert len(valid_generic_xml_source_record.xpath("//apple")) == 0


def test_xml_source_record_xpath_syntax_variations_valid(valid_generic_xml_source_record):
    single_line_expression = "//plants:fruits/plants:apples/plants:apple/plants:color"
    multi_line_expression = """
    //plants:fruits
        /plants:apples
            /plants:apple
                /plants:color
    """
    assert valid_generic_xml_source_record.xpath(
        single_line_expression
    ) == valid_generic_xml_source_record.xpath(multi_line_expression)


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
    assert valid_generic_xml_source_record.string_list_from_xpath("//miss") is None
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
    minimal_mitaardvark_record, minimal_mitaardvark_data
):
    assert minimal_mitaardvark_record.to_dict() == minimal_mitaardvark_data


def test_mitaardvark_to_json_success(
    minimal_mitaardvark_record, minimal_mitaardvark_data
):
    assert minimal_mitaardvark_record.to_json() == json.dumps(
        minimal_mitaardvark_data, indent=2
    )
    assert minimal_mitaardvark_record.to_json(pretty=False) == json.dumps(
        minimal_mitaardvark_data
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
    assert fgdc_source_record_from_zip._gbl_mdModified_dt() == "2024-01-01"


def test_record_shared_field_method_gbl_mdVersion_s_success(
    fgdc_source_record_from_zip,
):
    assert fgdc_source_record_from_zip._gbl_mdVersion_s() == "Aardvark"


def test_record_shared_field_method_dct_references_s_success(
    fgdc_source_record_from_zip,
):
    references = {
        "https://schema.org/downloadUrl": [
            {
                "label": "Source Metadata",
                "protocol": "Download",
                "url": "https://cdn.dev.mitlibrary.net/geo/public"
                "/SDE_DATA_AE_A8GNS_2003.source.fgdc.xml",
            },
            {
                "label": "Normalized Metadata",
                "protocol": "Download",
                "url": "https://cdn.dev.mitlibrary.net/geo/public"
                "/SDE_DATA_AE_A8GNS_2003.normalized.aardvark.json",
            },
            {
                "label": "Data Zipfile",
                "protocol": "Download",
                "url": "https://cdn.dev.mitlibrary.net/geo/public"
                "/SDE_DATA_AE_A8GNS_2003.zip",
            },
        ]
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
