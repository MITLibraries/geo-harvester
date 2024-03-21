"""harvester.records.formats.marc"""

# ruff: noqa: N802

import logging
import re
from decimal import Decimal, getcontext
from typing import Literal, TypeAlias

from attrs import define, field

from harvester.records.record import MarcalyxSourceRecord

logger = logging.getLogger(__name__)


# Coordinate string expected in format "hdddmmss" (hemisphere-degrees-minutes-seconds)
#   - e.g. W1800000, E1800000, N0840000, N0840000
#   - https://www.loc.gov/marc/bibliographic/bd034.html
COORDINATE_STRING: TypeAlias = str

# regular expression to extract hemisphere, degree, minutes, and seconds from a
# coordinate string
COORD_REGEX = re.compile(
    r"""^(?P<hemisphere>[NSEW+-])?
         (?P<degrees>\d{3}(\.\d*)?)
         (?P<minutes>\d{2}(\.\d*)?)?
         (?P<seconds>\d{2}(\.\d*)?)?""",
    re.IGNORECASE | re.VERBOSE,
)


@define
class MARC(MarcalyxSourceRecord):
    """MARC metadata format SourceRecord class."""

    metadata_format: Literal["marc"] = field(default="marc")

    ##########################
    # Required Field Methods
    ##########################

    def _dct_accessRights_s(self) -> str:
        """Field method dct_accessRights_s"""
        return "Public"

    def _dct_title_s(self) -> str | None:
        return self.marc.titleStatement().value.strip()

    def _gbl_resourceClass_sm(self) -> list[str]:
        """Field method: gbl_resourceClass_sm

        Controlled vocabulary:
            - 'Datasets'
            - 'Maps'
            - 'Imagery'
            - 'Collections'
            - 'Websites'
            - 'Web services'
            - 'Other'
        """
        return ["Maps"]

    def _dcat_bbox(self) -> str | None:
        """Field method: dcat_bbox"""
        bbox_data = self._get_largest_bounding_box()
        if bbox_data is None:
            return None

        return (
            f"ENVELOPE({bbox_data['w']}, "
            f"{bbox_data['e']}, "
            f"{bbox_data['n']}, "
            f"{bbox_data['s']})"
        )

    def _locn_geometry(self) -> str | None:
        """Field method: locn_geometry

        Some MARC records have a set of coordinates in 034 where the E/W and N/S
        coordinates are the same, effectively defining a WKT POINT.  If this is true
        across even multiple 034 tags, return a WKT POINT for this field.
        """
        bbox_data = self._get_largest_bounding_box()
        if bbox_data is None:
            return None

        if bbox_data["w"] == bbox_data["e"] and bbox_data["n"] == bbox_data["s"]:
            return f"POINT({bbox_data['w']}, {bbox_data['n']})"

        return self._dcat_bbox()

    ##########################
    # Optional Field Methods
    ##########################

    def _dct_description_sm(self) -> list[str]:
        return []

    def _dcat_keyword_sm(self) -> list[str]:
        """New field in Aardvark: no mapping from GBL1 to dcat_keyword_sm."""
        return []

    def _dct_alternative_sm(self) -> list[str]:
        """New field in Aardvark: no mapping from GBL1 to dct_alternative_sm."""
        return []

    def _dct_creator_sm(self) -> list[str] | None:
        return None

    def _dct_format_s(self) -> str | None:
        return None

    def _dct_issued_s(self) -> str | None:
        return None

    def _dct_identifier_sm(self) -> list[str]:
        return []

    def _dct_language_sm(self) -> list[str]:
        return []

    def _dct_publisher_sm(self) -> list[str]:
        return []

    def _dct_rights_sm(self) -> list[str]:
        return []

    def _dct_spatial_sm(self) -> list[str] | None:
        return None

    def _dct_subject_sm(self) -> list[str] | None:
        return None

    def _dct_temporal_sm(self) -> list[str] | None:
        return None

    def _gbl_dateRange_drsim(self) -> list[str]:
        return []

    def _gbl_resourceType_sm(self) -> list[str]:
        return []

    def _gbl_indexYear_im(self) -> list[int]:
        return []

    ##########################
    # Helpers
    ##########################

    def _get_largest_bounding_box(
        self,
    ) -> dict[Literal["w", "e", "n", "s"], Decimal] | None:
        """Method to return largest bounding box from 034 tags.

        Subfield mapping:
            $d - Coordinates - westernmost longitude (NR)
            $e - Coordinates - easternmost longitude (NR)
            $f - Coordinates - northernmost latitude (NR)
            $g - Coordinates - southernmost latitude (NR)
        """
        subfield_to_direction = {"d": "w", "e": "e", "f": "n", "g": "s"}

        # parse and filter 034 tags
        tags = self.marc.field("034")
        tags = [
            tag
            for tag in tags
            if all(tag.subfield(subfield) for subfield in subfield_to_direction)
        ]
        if not tags:
            message = "Record does not have valid 034 tag(s), cannot determine bbox."
            logger.debug(message)
            return None

        # build dictionary of all corner values
        bbox_data: dict[str, list] = {
            direction: [] for direction in subfield_to_direction.values()
        }
        for tag in tags:
            for subfield, direction in subfield_to_direction.items():
                value = self.convert_coordinate_string_to_decimal(
                    tag.subfield(subfield)[0].value
                )
                if value is not None:
                    bbox_data[direction].append(value)

        # return None if any four corners do not have values
        if not all(bbox_data.values()):
            return None

        # return largest box
        return {
            "w": min(bbox_data["w"]),
            "e": max(bbox_data["e"]),
            "n": max(bbox_data["n"]),
            "s": min(bbox_data["s"]),
        }

    @classmethod
    def pad_coordinate_string(cls, coordinate_string: COORDINATE_STRING) -> str:
        """Pad coordinate string with zeros."""
        hemisphere, coordinate = coordinate_string[0], coordinate_string[1:]
        if hemisphere in "NSEW":
            coordinate = f"{coordinate:>07}"
        return hemisphere + coordinate

    @classmethod
    def convert_coordinate_string_to_decimal(
        cls,
        coordinate_string: COORDINATE_STRING,
        precision: int = 10,
    ) -> Decimal | None:
        """Convert string coordinate to 10 digit precision decimal."""
        # get original global decimal precision and set temporarily
        original_precision = getcontext().prec
        getcontext().prec = precision

        # extract coordinate parts
        matches = COORD_REGEX.search(cls.pad_coordinate_string(coordinate_string))
        if not matches:
            return None
        parts = matches.groupdict()

        # construct decimal
        dec = (
            Decimal(parts.get("degrees"))  # type: ignore[arg-type]
            + Decimal(parts.get("minutes") or 0) / 60
            + Decimal(parts.get("seconds") or 0) / 3600
        )
        if parts.get("hemisphere") and parts["hemisphere"].lower() in "ws-":
            dec = dec * -1

        # reset global decimal precision
        getcontext().prec = original_precision

        return dec
