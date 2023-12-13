import datetime

from dateutil.parser import parse
from dateutil.tz import tzutc  # type: ignore[import-untyped]


def convert_to_utc(datetime_obj: datetime.datetime) -> datetime.datetime:
    """Convert a datetime object to UTC timezone aware."""
    return datetime_obj.astimezone(tzutc())


def date_parser(date_string: str) -> datetime.datetime:
    """Utility function to parse a date string into a datetime object.

    This provides a default datetime of "0001-01-01" which fills in gaps of the date
    parsed.  For example, the date string "2022" would parse to "2022-01-01" with the
    default provided.  Without a default, it would fill in the CURRENT month and day (or
    whatever date components were missing).
    """
    return parse(date_string, default=datetime.datetime(1, 1, 1, tzinfo=datetime.UTC))


def dedupe_list_of_strings(list_of_strings: list[str]) -> list[str]:
    """Utility function to dedupe list of strings regardless of casing or whitespace.

    If duplicates are found, this utility has a preference order of:
        - TitleCase
        - UPPERCASE
        - lowercase
    """
    if list_of_strings == []:
        return list_of_strings
    temp_dict = {string.lower().strip(): string.strip() for string in list_of_strings}
    dict_final_strings = {}
    for key in temp_dict:
        # TitleCase
        if temp_dict[key].istitle():  # noqa: SIM114
            dict_final_strings[key] = temp_dict[key]
        # UPPERCASE
        elif temp_dict[key].isupper():
            dict_final_strings[key] = temp_dict[key]
        # lowercase or others
        else:
            dict_final_strings[key] = temp_dict[key]
    return list(dict_final_strings.values())
