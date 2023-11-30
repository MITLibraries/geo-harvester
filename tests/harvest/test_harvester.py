import datetime
from unittest.mock import MagicMock

import pytest
from dateutil.parser import ParserError
from dateutil.tz import tzutc


@pytest.mark.usefixtures("_unset_s3_cdn_env_vars")
def test_harvester_harvest_missing_env_vars_raise_error(generic_harvester_class):
    harvester = generic_harvester_class(harvest_type="full")
    with pytest.raises(
        RuntimeError,
        match="Env vars S3_RESTRICTED_CDN_ROOT, S3_PUBLIC_CDN_ROOT must be set.",
    ):
        harvester.harvest()


def test_harvester_bad_harvest_type_raise_error(generic_harvester_class):
    harvester = generic_harvester_class(
        harvest_type="bad_type",
    )
    with pytest.raises(ValueError, match="harvest type: 'bad_type' not recognized"):
        harvester.harvest()


def test_harvester_from_until_date_parsing_success(generic_harvester_class):
    from_date = "2000-01-01"
    until_date = "2050-12-31"
    harvester = generic_harvester_class(
        from_date=from_date,
        until_date=until_date,
    )
    assert harvester.from_datetime_object == datetime.datetime(
        2000, 1, 1, 5, 0, tzinfo=tzutc()
    )
    assert harvester.until_datetime_object == datetime.datetime(
        2050, 12, 31, 5, 0, tzinfo=tzutc()
    )


def test_harvester_from_until_date_parsing_bad_date_raise_error(generic_harvester_class):
    harvester = generic_harvester_class(
        from_date="watermelon",
    )
    with pytest.raises(ParserError):
        _ = harvester.from_datetime_object
    assert harvester.until_datetime_object is None


def test_harvester_from_until_date_parsing_none_date_returns_none(
    generic_harvester_class,
):
    harvester = generic_harvester_class()
    assert harvester.from_datetime_object is None
    assert harvester.until_datetime_object is None


def test_harvester_harvest_type_selector_full_success(generic_harvester_class):
    harvester = generic_harvester_class(harvest_type="full")
    harvester.full_harvest = MagicMock()
    harvester.harvest()
    harvester.full_harvest.assert_called()


def test_harvester_harvest_type_selector_incremental_success(generic_harvester_class):
    harvester = generic_harvester_class(harvest_type="incremental")
    harvester.incremental_harvest = MagicMock()
    harvester.harvest()
    harvester.incremental_harvest.assert_called()


def test_harvester_harvest_type_selector_bad_type_raise_error(generic_harvester_class):
    harvester = generic_harvester_class(harvest_type="invalid-type")
    harvester.incremental_harvest = MagicMock()
    with pytest.raises(ValueError, match="harvest type: 'invalid-type' not recognized"):
        harvester.harvest()
