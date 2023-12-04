"""harvester.harvest"""
import datetime
import logging
from abc import ABC, abstractmethod

from attrs import define, field
from dateutil.parser import parse as date_parser

from harvester.config import Config
from harvester.utils import convert_to_utc

logger = logging.getLogger(__name__)

CONFIG = Config()


@define
class Harvester(ABC):
    """Generic Harvester class extended by MITHarvester and OGMHarvester."""

    harvest_type: str = field(default=None)
    from_date: str = field(default=None)
    until_date: str = field(default=None)

    @property
    def from_datetime_object(self) -> datetime.datetime | None:
        """Parses from date with UTC timezone offset set."""
        if self.from_date:
            return convert_to_utc(date_parser(self.from_date))
        return None

    @property
    def until_datetime_object(self) -> datetime.datetime | None:
        """Parses until date with UTC timezone offset set."""
        if self.until_date:
            return convert_to_utc(date_parser(self.until_date))
        return None

    def harvest(self) -> None:
        # ensure required env vars for MIT and OGM harvests are set
        if not all([CONFIG.S3_RESTRICTED_CDN_ROOT, CONFIG.S3_PUBLIC_CDN_ROOT]):
            message = "Env vars S3_RESTRICTED_CDN_ROOT, S3_PUBLIC_CDN_ROOT must be set."
            raise RuntimeError(message)

        if self.harvest_type == "full":
            harvest_result = self.full_harvest()
        elif self.harvest_type == "incremental":
            harvest_result = self.incremental_harvest()
        else:
            message = f"harvest type: '{self.harvest_type}' not recognized"
            raise ValueError(message)

        # NOTE: placeholder until harvest result handling established
        logger.info(harvest_result)

    @abstractmethod
    def full_harvest(self) -> None:
        pass  # pragma: nocover

    @abstractmethod
    def incremental_harvest(self) -> None:
        pass  # pragma: nocover
