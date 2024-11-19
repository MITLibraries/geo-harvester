"""harvester.aws.eventbridge"""

import json
import logging
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from mypy_boto3_events.client import (
        EventBridgeClient as EventBridgeClientType,
    )  # pragma: nocover


logger = logging.getLogger(__name__)


class EventBridgeClient:
    @classmethod
    def get_client(cls) -> "EventBridgeClientType":
        return boto3.client("events")

    @classmethod
    def send_event(cls, detail: dict) -> str:
        """Send EventBridge event."""
        response = cls.get_client().put_events(
            Entries=[
                {
                    "Detail": json.dumps(detail),
                    "DetailType": "geo-harvester run",
                    "Source": "geo-harvester.app",
                    "EventBusName": "default",
                },
            ]
        )
        created_event_id = response["Entries"][0]["EventId"]
        message = f"EventBridge event created: {created_event_id}"
        logger.debug(message)
        return created_event_id
