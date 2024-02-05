from harvester.records.validators import ValidateGeoshapeWKT


def test_validator_envelope_returns_original_value_success(
    caplog,
):
    caplog.set_level("DEBUG")

    class TestValidatedXMLSourceRecord:
        @ValidateGeoshapeWKT
        def _dcat_bbox(self):
            return "ENVELOPE(71.0589, 74.0060, 42.3601, 40.7128)"

    value = TestValidatedXMLSourceRecord()._dcat_bbox()  # noqa: SLF001
    assert value == "ENVELOPE(71.0589, 74.0060, 42.3601, 40.7128)"


def test_validator_envelope_missing_vertices_logs_warning_and_sets_value_to_none(
    caplog,
):
    caplog.set_level("DEBUG")

    class TestValidatedXMLSourceRecord:
        @ValidateGeoshapeWKT
        def _dcat_bbox(self):
            return "ENVELOPE()"

    value = TestValidatedXMLSourceRecord()._dcat_bbox()  # noqa: SLF001
    invalid_value_warning_message = (
        "field: dcat_bbox, shapely was unable to parse WKT: 'ENVELOPE()'; "
        "setting value to None"
    )
    assert value is None
    assert invalid_value_warning_message in caplog.text


def test_validator_envelope_insufficient_vertices_logs_warning_and_sets_value_to_none(
    caplog,
):
    caplog.set_level("DEBUG")

    class TestValidatedXMLSourceRecord:
        @ValidateGeoshapeWKT
        def _dcat_bbox(self):
            return "ENVELOPE(71.0589, 74.0060, 42.3601)"

    value = TestValidatedXMLSourceRecord()._dcat_bbox()  # noqa: SLF001
    invalid_value_warning_message = (
        "field: dcat_bbox, shapely was unable to parse WKT: "
        "'ENVELOPE(71.0589, 74.0060, 42.3601)'; "
        "setting value to None"
    )
    assert value is None
    assert invalid_value_warning_message in caplog.text


def test_validator_polygon_returns_original_value_success(
    caplog,
):
    caplog.set_level("DEBUG")
    polygon_wkt = (
        "POLYGON ((74.0060 40.7128, 71.0589 42.3601, "
        "73.7562 42.6526, 74.0060 40.7128))"
    )

    class TestValidatedXMLSourceRecord:
        @ValidateGeoshapeWKT
        def _locn_geometry(self):
            return polygon_wkt

    value = TestValidatedXMLSourceRecord()._locn_geometry()  # noqa: SLF001
    assert value == polygon_wkt


def test_validator_polygon_missing_vertices_logs_warning_and_sets_value_to_none(
    caplog,
):
    caplog.set_level("DEBUG")

    class TestValidatedXMLSourceRecord:
        @ValidateGeoshapeWKT
        def _locn_geometry(self):
            return "POLYGON (())"

    value = TestValidatedXMLSourceRecord()._locn_geometry()  # noqa: SLF001
    invalid_value_warning_message = (
        "field: locn_geometry, shapely was unable to parse WKT: 'POLYGON (())'; "
        "setting value to None"
    )
    assert value is None
    assert invalid_value_warning_message in caplog.text


def test_validator_multipolygon_returns_original_value_success(
    caplog,
):
    caplog.set_level("DEBUG")
    multipolygon_wkt = (
        "MULTIPOLYGON (((40.7128 74.0060, 42.3601 71.0589, 42.6526 73.7562, "
        "40.7128 74.0060), (41.7658 72.6734, 41.5623 72.6506, 41.5582 73.0515, "
        "41.7658 72.6734)), ((73.9776 40.7614, 73.9554 40.7827, 73.9631 40.7812, "
        "73.9776 40.7614)))"
    )

    class TestValidatedXMLSourceRecord:
        @ValidateGeoshapeWKT
        def _locn_geometry(self):
            return multipolygon_wkt

    value = TestValidatedXMLSourceRecord()._locn_geometry()  # noqa: SLF001
    assert value == multipolygon_wkt


def test_validator_multipolygon_missing_vertices_logs_warning_and_sets_value_to_none(
    caplog,
):
    caplog.set_level("DEBUG")

    class TestValidatedXMLSourceRecord:
        @ValidateGeoshapeWKT
        def _locn_geometry(self):
            return "MULTIPOLYGON (((), ()), (()))"

    value = TestValidatedXMLSourceRecord()._locn_geometry()  # noqa: SLF001
    invalid_value_warning_message = (
        "field: locn_geometry, shapely was unable to parse WKT: "
        "'MULTIPOLYGON (((), ()), (()))'; "
        "setting value to None"
    )
    assert value is None
    assert invalid_value_warning_message in caplog.text
