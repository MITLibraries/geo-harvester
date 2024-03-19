"""harvester.records.sources.mit"""

import json
import logging
from typing import Literal

from attrs import define, field

from harvester.aws.sqs import ZipFileEventMessage
from harvester.config import Config
from harvester.records import SourceRecord
from harvester.records.formats import FGDC, ISO19139

logger = logging.getLogger(__name__)

CONFIG = Config()


@define(slots=False)
class MITSourceRecord(SourceRecord):
    """Class to extend SourceRecord for harvested GIS team Zip files.

    Extended Args:
        zip_file_location: path string to the zip file
            - this may be local or S3 URI
        sqs_message: ZipFileEventMessage instance
            - present only for MIT harvests
            - by affixing to SourceRecord during record retrieval, it allows for use
            after the record has been processed to manage the message in the queue
    """

    origin: Literal["mit"] = field(default="mit")
    zip_file_location: str = field(default=None)
    sqs_message: ZipFileEventMessage = field(default=None)

    def _dct_references_s(self) -> str:
        """Create dct_references_s JSON string for MIT harvests.

        For MIT harvests, this includes the data zip file, source and normalized metadata
        records in CDN, and a link to the TIMDEX item page.
        """
        cdn_folder = {True: "restricted", False: "public"}[self.is_restricted]
        cdn_root = CONFIG.http_cdn_root
        download_urls = [
            {
                "label": "Source Metadata",
                "url": f"{cdn_root}/public/{self.source_metadata_filename}",
            },
            {
                "label": "Aardvark Metadata",
                "url": f"{cdn_root}/public/{self.normalized_metadata_filename}",
            },
            {
                "label": "Data",
                "url": f"{cdn_root}/{cdn_folder}/{self.identifier}.zip",
            },
        ]
        website_url = (
            "https://geodata.libraries.mit.edu/record/"
            f"gismit:{self.identifier.removeprefix('mit:')}"
        )
        return json.dumps(
            {
                "http://schema.org/downloadUrl": download_urls,
                "http://schema.org/url": website_url,
            }
        )

    def _schema_provider_s(self) -> str:
        """Shared field method: schema_provider_s"""
        return "GIS Lab, MIT Libraries"


@define(slots=False)
class MITFGDC(MITSourceRecord, FGDC):
    pass


@define(slots=False)
class MITISO19139(MITSourceRecord, ISO19139):
    pass
