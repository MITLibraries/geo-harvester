import logging
import re
from ast import literal_eval
from collections.abc import Callable
from functools import partial, update_wrapper

import shapely

from harvester.records.exceptions import FieldValueInvalidWarning

logger = logging.getLogger(__name__)


##########################
# Data Validators
##########################
class ValidateGeoshapeWKT:
    """Decorator class for validating geoshape WKT values.

    The validator should be applied to any field methods that retrieve geoshape
    WKT values. The validator logs a warning if the WKT value cannot be parsed
    using the shapely module. If the WKT value cannot be parsed, the validator
    resets the value to None.
    """

    invalid_wkt_warning_message: str = (
        "field: {field_name}, shapely was unable to parse WKT: '{value}'; "
        "setting value to None"
    )

    def __init__(self, field_method: Callable):
        update_wrapper(self, field_method)
        self.field_method = field_method

    def __call__(self, obj: object) -> str | None:
        field_name = self.field_method.__name__.removeprefix("_")
        value = self.field_method(obj)

        try:
            self.create_geoshape(value)
        except Exception:  # noqa: BLE001
            logger.warning(
                FieldValueInvalidWarning(
                    self.invalid_wkt_warning_message.format(
                        field_name=field_name, value=value
                    )
                )
            )
            return None
        return value

    def __get__(self, obj: object, _: type) -> partial[str | None]:
        """Required by decorator to access SourceRecord instance"""
        return partial(self.__call__, obj)

    @staticmethod
    def create_geoshape(wkt: str) -> shapely.Geometry:
        """Run shapely to determine whether geoshape WKT value is valid.

        If the geoshape WKT value is valid, shapely can successfully create a
        geometry object.

        Note: shapely does not currently support WKT values for bounding box regions or
        envelopes. The method uses regex to retrieving the vertices for a geoshape WKT
        value with the format: "ENVELOPE(<vertices>)". The regex retrieves the vertices
        inside the parentheses and passes the vertices as arguments to shapely.box().
        """
        if geoshape_string := re.compile(r"^ENVELOPE\s?(.*)").match(wkt):
            xmin, xmax, ymax, ymin = literal_eval(geoshape_string.group(1))
            return shapely.box(xmin, ymin, xmax, ymax)
        return shapely.from_wkt(wkt)
