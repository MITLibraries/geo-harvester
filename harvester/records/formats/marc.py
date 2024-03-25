"""harvester.records.formats.marc"""

# ruff: noqa: N802

import logging
import re
from collections import defaultdict
from decimal import Decimal, getcontext
from typing import Literal, TypeAlias

from attrs import define, field
from marcalyx.marcalyx import DataField  # type: ignore[import-untyped]

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

TAG_034_SUBFIELD_TO_DIRECTION = {"d": "w", "e": "e", "f": "n", "g": "s"}


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

        Aardvark controlled vocabulary:
        https://opengeometadata.org/ogm-aardvark/#resource-class
            - 'Datasets'
            - 'Maps'
            - 'Imagery'
            - 'Collections'
            - 'Websites'
            - 'Web services'
            - 'Other'
        """
        tag_336_to_aardvark_map = {
            "cartographic dataset": "Datasets",
            "cartographic images": "Maps",
            "text": "Other",
            "unspecified": "Other",
            "still image": "Imagery",
            "computer dataset": "Datasets",
            "cartographic image": "Imagery",
            "cartographic three-dimensional form": "Other",
        }

        tag_336_value = self.get_single_tag_subfield_value("336", "a")
        return [tag_336_to_aardvark_map[tag_336_value]]

    def _dcat_bbox(self) -> str | None:
        """Field method: dcat_bbox"""
        bbox_data = self.get_largest_bounding_box()
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
        bbox_data = self.get_largest_bounding_box()
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

    def get_largest_bounding_box(
        self,
    ) -> dict[Literal["w", "e", "n", "s"], Decimal] | None:
        """Determine largest bounding box from potentially multiple bounding boxes.

        The MARC 034 tag is where geographic bounding boxes are found, and it is allowed
        to repeat.  This method generates the largest bounding box that covers all
        bounding boxes in the form of a dictionary {direction: decimal}.
        """
        tags = self._bbox_get_valid_034_tags()
        bbox_data = self._bbox_extract_data_from_tags(tags)
        return self._bbox_validate_and_parse_max_min_data(bbox_data)

    def _bbox_get_valid_034_tags(self) -> list[DataField]:
        """Return 034 tags that have bounding box subfields."""
        return [
            tag
            for tag in self.marc.field("034")
            if all(tag.subfield(subfield) for subfield in TAG_034_SUBFIELD_TO_DIRECTION)
        ]

    @classmethod
    def _bbox_extract_data_from_tags(cls, tags: list) -> defaultdict[str, list[Decimal]]:
        """Extract decimal values for all corners of all bounding boxes."""
        bbox_data = defaultdict(list)
        for tag in tags:
            for subfield_code, direction in TAG_034_SUBFIELD_TO_DIRECTION.items():
                if subfield := cls.get_single_subfield(tag, subfield_code):
                    value = cls._bbox_convert_coordinate_string_to_decimal(subfield.value)
                    if value is not None:
                        bbox_data[direction].append(value)
        return bbox_data

    @staticmethod
    def _bbox_validate_and_parse_max_min_data(
        bbox_data: defaultdict[str, list[Decimal]],
    ) -> dict[Literal["w", "e", "n", "s"], Decimal] | None:
        """Validate data and return max/min decimal values for all corners."""
        for direction in TAG_034_SUBFIELD_TO_DIRECTION.values():
            if len(bbox_data[direction]) == 0:
                return None
        return {
            "w": min(bbox_data["w"]),
            "e": max(bbox_data["e"]),
            "n": max(bbox_data["n"]),
            "s": min(bbox_data["s"]),
        }

    @staticmethod
    def _bbox_pad_coordinate_string(coordinate_string: COORDINATE_STRING) -> str:
        """Pad coordinate string with zeros."""
        hemisphere, coordinate = coordinate_string[0], coordinate_string[1:]
        if hemisphere in "NSEW":
            coordinate = f"{coordinate:>07}"
        return hemisphere + coordinate

    @classmethod
    def _bbox_convert_coordinate_string_to_decimal(
        cls, coordinate_string: COORDINATE_STRING, precision: int = 10
    ) -> Decimal | None:
        """Convert string coordinate to 10 digit precision decimal.

        This method accepts a coordinate string in the format "hdddmmss"
        (hemisphere-degrees-minutes-seconds) and converts it into a 10 digit decimal
        string that is appropriate for a Well-Known-Text (WKT) string.  See the type
        COORDINATE_STRING at the top for more information about the source format.
        """
        # get original global decimal precision and set temporarily
        original_precision = getcontext().prec
        getcontext().prec = precision

        # extract coordinate parts
        matches = COORD_REGEX.search(cls._bbox_pad_coordinate_string(coordinate_string))
        if not matches:
            return None
        parts = matches.groupdict()

        # construct decimal value
        decimal_value = (
            Decimal(parts.get("degrees"))  # type: ignore[arg-type]
            + Decimal(parts.get("minutes") or 0) / 60
            + Decimal(parts.get("seconds") or 0) / 3600
        )
        if parts.get("hemisphere") and parts["hemisphere"].lower() in "ws-":
            decimal_value = decimal_value * -1

        # reset global decimal precision
        getcontext().prec = original_precision

        return decimal_value
