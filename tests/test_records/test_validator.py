import pytest

from unittest.mock import MagicMock

from harvester.records.validators import ValidateGeoshapeWKT


def test_validator_invalid_geoshape_wkt_logs_warning_and_resets_value(caplog):
    caplog.set_level("DEBUG")
    mock_dcat_bbox = MagicMock()
    mock_dcat_bbox.name = "_dcat_bbox"
    mock_dcat_bbox.return_value = "ENVELOPE"
    value = ValidateGeoshapeWKT(mock_dcat_bbox)()
    assert (
        "field: dcat_bbox, unable to parse WKT: 'ENVELOPE'; setting value to None"
    ) in caplog.text
    assert value is None
