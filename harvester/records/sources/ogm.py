"""harvester.records.sources.ogm"""

import json
import logging
from typing import Literal

from attrs import define, field

from harvester.config import Config
from harvester.records import SourceRecord
from harvester.records.exceptions import NoExternalUrlError
from harvester.records.formats import FGDC, GBL1, ISO19139, Aardvark

logger = logging.getLogger(__name__)

CONFIG = Config()


@define(slots=False)
class OGMSourceRecord(SourceRecord):
    """Class to extend SourceRecord for harvested OGM metadata records.

    Extended Args:
        ogm_repo_config: config dictionary of OGM repository from configuration YAML
    """

    origin: Literal["ogm"] = field(default="ogm")
    ogm_repo_config: dict = field(default=None)

    def _schema_provider_s(self) -> str:
        """Shared field method: schema_provider_s"""
        return self.ogm_repo_config["name"]


@define(slots=False)
class OGMFGDC(OGMSourceRecord, FGDC):
    def _dct_references_s(self) -> str:
        # Currently not harvesting FGDC from OGM, this would need defining if so
        raise NotImplementedError


@define(slots=False)
class OGMISO19139(OGMSourceRecord, ISO19139):
    def _dct_references_s(self) -> str:
        # Currently not harvesting ISO19139 from OGM, this would need defining if so
        raise NotImplementedError


@define(slots=False)
class OGMGBL1(OGMSourceRecord, GBL1):
    def _dct_references_s(self) -> str:
        """Field method helper: "dct_references_s"

        For most OGM repositories providing GBL1 metadata, pulling this URL from the
        dct_references_s field directly suffices, and this approach is therefore the
        default.  However, some repositories require alternate strategies which can be
        optionally defined in the OGM config YAML using the "external_url_strategy"
        property.

        Additionally, if the URI "http://schema.org/downloadUrl" is present, and only a
        single value, use.  If value is array, skip, as we cannot be sure of a single
        download link to choose from.
        """
        # extract required external url
        url: None | str
        if external_url_strategy := self.ogm_repo_config.get("external_url_strategy"):
            url = self._use_external_url_strategy(external_url_strategy)
        else:
            refs_dict = json.loads(self.parsed_data["dct_references_s"])
            url = refs_dict.get("http://schema.org/url")
        if not url:
            raise NoExternalUrlError
        urls_dict = {"http://schema.org/url": url}

        # extract optional download url
        download_uri = "http://schema.org/downloadUrl"
        refs_dict = json.loads(self.parsed_data["dct_references_s"])
        if download_value := refs_dict.get(download_uri):  # noqa: SIM102
            if isinstance(download_value, str):
                urls_dict[download_uri] = [  # type: ignore[assignment]
                    {
                        "label": "Data",
                        "url": download_value,
                    }
                ]

        return json.dumps(urls_dict)

    def _use_external_url_strategy(self, alternate_strategy: dict) -> str:
        """Apply alternative strategy for extracting external URL from source record.

        OGM repositories may include an optional "external_url_strategy" property where
        a sub-property "name" defines the strategy to use.  Currently supported:

            - "base_url_and_slug": a pre-defined base URL is combined with the value from
            a defined field to construct a URL

            - "field_value": a single field contains a full URL
        """
        url: None | str
        strategy_name = alternate_strategy["name"]
        if strategy_name == "base_url_and_slug":
            url = "/".join(
                [
                    alternate_strategy["base_url"],
                    self.parsed_data[alternate_strategy["gbl1_field"]],
                ]
            )
        elif strategy_name == "field_value":
            url = self.parsed_data.get(alternate_strategy["gbl1_field"])
            if url and not url.startswith("http"):
                url = None
        else:
            error_message = f"Alternate URL strategy not recognized: {strategy_name}"
            raise ValueError(error_message)
        return url


@define(slots=False)
class OGMAardvark(OGMSourceRecord, Aardvark):
    def _dct_references_s(self) -> str:
        """Field method helper: "dct_references_s"

        For OGM repositories that provide Aardvark metadata, the most reliable location
        to find an external URL is the 'http://schema.org/url' key in the dct_references_s
        JSON payload.

        If the URI "http://schema.org/downloadUrl" is present, and only a single value,
        use.  If array, skip, as cannot be sure of a single download link to choose from.
        """
        refs_dict = json.loads(self.parsed_data["dct_references_s"])

        # extract required external URL
        url = refs_dict.get("http://schema.org/url")
        if not url:
            raise NoExternalUrlError
        urls_dict = {"http://schema.org/url": url}

        # extract optional download url
        download_uri = "http://schema.org/downloadUrl"
        if download_value := refs_dict.get(download_uri):  # noqa: SIM102
            if isinstance(download_value, str):
                urls_dict[download_uri] = [
                    {
                        "label": "Data",
                        "url": download_value,
                    }
                ]

        return json.dumps(urls_dict)
