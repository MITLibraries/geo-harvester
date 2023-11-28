"""harvester.harvest"""
import datetime
import logging
from abc import ABC, abstractmethod

from attrs import define, field
from dateutil.parser import parse as date_parser

from harvester.utils import convert_to_utc

logger = logging.getLogger(__name__)


@define
class Harvester(ABC):
    """Generic Harvester class extended by MITHarvester and OGMHarvester."""

    harvest_type: str = field(default=None)
    from_date: str = field(default=None)
    until_date: str = field(default=None)

    def harvest(self) -> None:
        if self.harvest_type == "full":
            return self.full_harvest()
        if self.harvest_type == "incremental":
            return self.incremental_harvest()
        message = f"harvest type: '{self.harvest_type}' not recognized"
        raise ValueError(message)

    @property
    def from_datetime_obj(self) -> datetime.datetime | None:
        """Parses from date with UTC timezone offset set."""
        if self.from_date:
            return convert_to_utc(date_parser(self.from_date))
        return None

    @property
    def until_datetime_obj(self) -> datetime.datetime | None:
        """Parses until date with UTC timezone offset set."""
        if self.until_date:
            return convert_to_utc(date_parser(self.until_date))
        return None

    @abstractmethod
    def full_harvest(self) -> None:
        pass  # pragma: nocover

    @abstractmethod
    def incremental_harvest(self) -> None:
        pass  # pragma: nocover
