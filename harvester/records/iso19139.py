"""harvester.harvest.records.iso19139"""

from typing import Literal

from attrs import define, field

from harvester.records.record import XMLSourceRecord


@define
class ISO19139(XMLSourceRecord):
    """ISO19139 metadata format SourceRecord class."""

    metadata_format: Literal["iso19139"] = field(default="iso19139")
    nsmap: dict = field(
        default={
            "gmd": "http://www.isotc211.org/2005/gmd",
            "gco": "http://www.isotc211.org/2005/gco",
            "gts": "http://www.isotc211.org/2005/gts",
            "srv": "http://www.isotc211.org/2005/srv",
            "gml": "http://www.opengis.net/gml",
        },
        repr=False,
    )

    def _dct_title_s(self) -> str | None:
        """WIP: improved when other fields added, but demonstrates general approach"""
        xpath_expr = """
        //gmd:MD_Metadata
            /gmd:identificationInfo
                /gmd:MD_DataIdentification
                    /gmd:citation
                        /gmd:CI_Citation
                            /gmd:title
                                /gco:CharacterString
        """
        titles = self.string_list_from_xpath(xpath_expr)
        if titles:
            return titles[0]
        return None
