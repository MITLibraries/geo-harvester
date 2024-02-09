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
    if not list_of_values:
        return list_of_values

    # handle edge case where OGM repositories set a single value as a list
    # when it should be a single, scalar value
    if (
        isinstance(list_of_values, list)
        and len(list_of_values) == 1
        and isinstance(list_of_values[0], list)
    ):
        list_of_values = list_of_values[0]

    temp_dict = {}

    for item in list_of_values:
        if isinstance(item, str):
            key = item.lower().strip()
            value = item.strip()
            # Add key if not yet seen
            if key not in temp_dict:
                temp_dict[key] = value
            else:  # noqa: PLR5501
                # If current value is TitleCase, overwrite the value in the dictionary.
                if value.istitle():  # noqa: SIM114
                    temp_dict[key] = value
                # If current value is UPPERCASE and the value in dictionary isn't
                # TitleCase, overwrite it.
                elif value.isupper() and not temp_dict[key].istitle():  # noqa: SIM114
                    temp_dict[key] = value
                # If the current value is lowercase and the dictionary value isn't
                # UPPERCASE or TitleCase, overwrite it.
                elif value.islower() and not (
                    temp_dict[key].isupper() or temp_dict[key].istitle()
                ):
                    temp_dict[key] = value
        else:
            # Handle non-string items
            temp_dict.setdefault(item, item)

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
