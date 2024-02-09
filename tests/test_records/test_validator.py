def test_validator_envelope_none_nonetype_skips_validation_logs_warning_returns_none(
    caplog, mocked_validated_fields_source_record
):
    caplog.set_level("DEBUG")
    mocked_validated_fields_source_record.mocked_value = None
    value = mocked_validated_fields_source_record()._dcat_bbox()  # noqa: SLF001
    invalid_wkt_warning_message = (
        "field: dcat_bbox, unable to parse WKT from value: None; returning None"
    )
    assert value is None
    assert invalid_wkt_warning_message in caplog.text


def test_validator_envelope_none_string_skips_validation_logs_warning_returns_none(
    caplog, mocked_validated_fields_source_record
):
    caplog.set_level("DEBUG")
    mocked_validated_fields_source_record.mocked_value = "None"
    value = mocked_validated_fields_source_record()._dcat_bbox()  # noqa: SLF001
    invalid_wkt_warning_message = (
        "field: dcat_bbox, unable to parse WKT from value: None; returning None"
    )
    assert value is None
    assert invalid_wkt_warning_message in caplog.text


def test_validator_envelope_nonstring_skips_validation_logs_warning_returns_none(
    caplog, mocked_validated_fields_source_record
):
    caplog.set_level("DEBUG")
    mocked_validated_fields_source_record.mocked_value = 999
    value = mocked_validated_fields_source_record()._dcat_bbox()  # noqa: SLF001
    invalid_wkt_warning_message = (
        "field: dcat_bbox, unable to parse WKT from value: 999; returning None"
    )
    assert value is None
    assert invalid_wkt_warning_message in caplog.text


def test_validator_envelope_returns_original_value_success(
    caplog, mocked_validated_fields_source_record
):
    caplog.set_level("DEBUG")
    mocked_validated_fields_source_record.mocked_value = (
        "ENVELOPE(71.0589, 74.0060, 42.3601, 40.7128)"
    )
    value = mocked_validated_fields_source_record()._dcat_bbox()  # noqa: SLF001
    assert value == "ENVELOPE(71.0589, 74.0060, 42.3601, 40.7128)"


def test_validator_envelope_missing_vertices_logs_warning_sets_value_to_none(
    caplog, mocked_validated_fields_source_record
):
    caplog.set_level("DEBUG")
    mocked_validated_fields_source_record.mocked_value = "ENVELOPE()"
    value = mocked_validated_fields_source_record()._dcat_bbox()  # noqa: SLF001
    invalid_wkt_warning_message = (
        "field: dcat_bbox, unable to parse WKT from value: ENVELOPE(); returning None"
    )
    assert value is None
    assert invalid_wkt_warning_message in caplog.text


def test_validator_envelope_insufficient_vertices_logs_warning_sets_value_to_none(
    caplog, mocked_validated_fields_source_record
):
    caplog.set_level("DEBUG")
    mocked_validated_fields_source_record.mocked_value = (
        "ENVELOPE(71.0589, 74.0060, 42.3601)"
    )
    value = mocked_validated_fields_source_record()._dcat_bbox()  # noqa: SLF001
    invalid_wkt_warning_message = (
        "field: dcat_bbox, unable to parse WKT from value: "
        "ENVELOPE(71.0589, 74.0060, 42.3601); returning None"
    )
    assert value is None
    assert invalid_wkt_warning_message in caplog.text


def test_validator_polygon_returns_original_value_success(
    caplog, mocked_validated_fields_source_record
):
    caplog.set_level("DEBUG")
    polygon_wkt = (
        "POLYGON ((74.0060 40.7128, 71.0589 42.3601, "
        "73.7562 42.6526, 74.0060 40.7128))"
    )
    mocked_validated_fields_source_record.mocked_value = polygon_wkt
    value = mocked_validated_fields_source_record()._locn_geometry()  # noqa: SLF001
    assert value == polygon_wkt


def test_validator_polygon_missing_vertices_logs_warning_sets_value_to_none(
    caplog, mocked_validated_fields_source_record
):
    caplog.set_level("DEBUG")
    mocked_validated_fields_source_record.mocked_value = "POLYGON (())"
    value = mocked_validated_fields_source_record()._locn_geometry()  # noqa: SLF001
    invalid_wkt_warning_message = (
        "field: dcat_bbox, unable to parse WKT from value: POLYGON (()); "
        "returning None"
    )
    assert value is None
    assert invalid_wkt_warning_message in caplog.text


def test_validator_multipolygon_returns_original_value_success(
    caplog, mocked_validated_fields_source_record
):
    caplog.set_level("DEBUG")
    multipolygon_wkt = (
        "MULTIPOLYGON (((40.7128 74.0060, 42.3601 71.0589, 42.6526 73.7562, "
        "40.7128 74.0060), (41.7658 72.6734, 41.5623 72.6506, 41.5582 73.0515, "
        "41.7658 72.6734)), ((73.9776 40.7614, 73.9554 40.7827, 73.9631 40.7812, "
        "73.9776 40.7614)))"
    )
    mocked_validated_fields_source_record.mocked_value = multipolygon_wkt
    value = mocked_validated_fields_source_record()._locn_geometry()  # noqa: SLF001
    assert value == multipolygon_wkt


def test_validator_multipolygon_missing_vertices_logs_warning_sets_value_to_none(
    caplog, mocked_validated_fields_source_record
):
    caplog.set_level("DEBUG")
    mocked_validated_fields_source_record.mocked_value = "MULTIPOLYGON (((), ()), (()))"
    value = mocked_validated_fields_source_record()._locn_geometry()  # noqa: SLF001
    invalid_wkt_warning_message = (
        "field: dcat_bbox, unable to parse WKT from value: "
        "MULTIPOLYGON (((), ()), (())); returning None"
    )
    assert value is None
    assert invalid_wkt_warning_message in caplog.text
