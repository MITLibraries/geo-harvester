from unittest.mock import MagicMock

import pytest
from dateutil.parser import ParserError
from dateutil.parser import parse as date_parser

from harvester.utils import convert_to_utc


@pytest.mark.usefixtures("_unset_s3_cdn_env_vars")
def test_harvester_harvest_missing_env_vars(generic_harvester_class):
    harvester = generic_harvester_class(harvest_type="full")
    with pytest.raises(
        RuntimeError,
        match="Env vars S3_RESTRICTED_CDN_ROOT, S3_PUBLIC_CDN_ROOT must be set.",
    ):
        harvester.harvest()


def test_harvester_bad_harvest_type(generic_harvester_class):
    harvester = generic_harvester_class(
        harvest_type="bad_type",
    )
    with pytest.raises(ValueError, match="harvest type: 'bad_type' not recognized"):
        harvester.harvest()


def test_harvester_from_until_date_parsing(generic_harvester_class):
    # valid dates
    from_date = "2000-01-01"
    until_date = "2050-12-31"
    harvester = generic_harvester_class(
        from_date=from_date,
        until_date=until_date,
    )

    # assert mismatch when UTC timezone not set
    with pytest.raises(AssertionError):
        assert harvester.from_datetime_obj == date_parser(from_date)
    with pytest.raises(AssertionError):
        assert harvester.until_datetime_obj == date_parser(until_date)

    # assert match with UTC set
    assert harvester.from_datetime_obj == convert_to_utc(date_parser(from_date))
    assert harvester.until_datetime_obj == convert_to_utc(date_parser(until_date))

    # invalid date string
    harvester = generic_harvester_class(
        from_date="watermelon",
    )
    with pytest.raises(ParserError):
        _ = harvester.from_datetime_obj
    assert harvester.until_datetime_obj is None

    # none dates
    harvester = generic_harvester_class()
    assert harvester.from_datetime_obj is None
    assert harvester.until_datetime_obj is None


def test_harvester_harvest_type_selector(generic_harvester_class):
    # full
    harvester = generic_harvester_class(harvest_type="full")
    harvester.full_harvest = MagicMock()
    harvester.harvest()
    harvester.full_harvest.assert_called()

    # incremental
    harvester = generic_harvester_class(harvest_type="incremental")
    harvester.incremental_harvest = MagicMock()
    harvester.harvest()
    harvester.incremental_harvest.assert_called()
