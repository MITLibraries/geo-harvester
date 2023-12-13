import datetime

import pycountry
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


def dedupe_list_of_values(list_of_values: list) -> list:
    """Utility function to dedupe list of values in a list.

    This function dedupes regardless of value casing (if string) or whitespace. If
    duplicates are found, this utility has a preference order of:
        - TitleCase
        - UPPERCASE
        - lowercase
    """
    if list_of_values == []:
        return list_of_values

    temp_dict = {}
    for item in list_of_values:
        if isinstance(item, str):
            key = item.lower().strip()
            value = item.strip()
            # TitleCase
            if value.istitle():  # noqa: SIM114; not applicable here
                temp_dict[key] = value
            # UPPERCASE
            elif value.isupper():
                temp_dict[key] = value
            # lowercase or others
            else:
                temp_dict[key] = value
        else:
            # handle non-string items
            temp_dict[item] = item

    return list(temp_dict.values())


def convert_lang_code(code: str) -> str | None:
    """Convert 2 or 3-letter language codes into 3-letter ISO 639-2 language codes."""
    if len(code) == 2:  # noqa: PLR2004
        lang = pycountry.languages.get(alpha_2=code)
    elif len(code) == 3:  # noqa: PLR2004
        lang = pycountry.languages.get(alpha_3=code)
    else:
        return None
    return lang.alpha_3 if lang else None
