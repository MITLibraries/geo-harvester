import datetime

from dateutil.tz import tzutc  # type: ignore[import-untyped]


def convert_to_utc(datetime_obj: datetime.datetime) -> datetime.datetime:
    return datetime_obj.astimezone(tzutc())
