"""harvester.aws.sqs"""

import datetime
import json
import logging
import os
from collections.abc import Iterator
from typing import TYPE_CHECKING, Literal

import boto3
from dateutil.parser import ParserError
from dateutil.parser import parse as date_parser

if TYPE_CHECKING:  # pragma: nocover
    from mypy_boto3_sqs.client import SQSClient as SQSClientType
    from mypy_boto3_sqs.type_defs import MessageTypeDef

from harvester.config import Config
from harvester.utils import convert_to_utc

logger = logging.getLogger(__name__)

CONFIG = Config()


class MessageValidationError(Exception):
    pass


class ZipFileEventMessage:
    """Class to represent SQS Message."""

    def __init__(self, raw: "MessageTypeDef"):
        self.raw = raw
        self.validate_message()

    @property
    def message_id(self) -> str:
        return self.raw["MessageId"]

    @property
    def receipt_handle(self) -> str:
        return self.raw["ReceiptHandle"]

    @property
    def body(self) -> dict:
        return json.loads(self.raw["Body"])

    @property
    def event(self) -> Literal["created", "deleted"]:
        """Return a normalized form of the S3 event for the file."""
        if self.body["detail-type"] == "Object Created":
            return "created"
        if self.body["detail-type"] == "Object Deleted":
            return "deleted"
        message = f"Message detail-type not recognized: {self.body['detail-type']}"
        raise AttributeError(message)

    @property
    def reason(self) -> str:
        return self.body["detail"]["reason"]

    @property
    def bucket(self) -> str:
        return self.body["detail"]["bucket"]["name"]

    @property
    def key(self) -> str:
        return self.body["detail"]["object"]["key"]

    @property
    def modified(self) -> datetime.datetime:
        return convert_to_utc(date_parser(self.body["time"]))

    @property
    def zip_file(self) -> str:
        """Generate full path of zip file in CDN restricted bucket."""
        if not CONFIG.S3_RESTRICTED_CDN_ROOT:
            message = (
                "Cannot determine CDN:Restricted path without env var "
                "S3_RESTRICTED_CDN_ROOT set"
            )
            raise ValueError(message)
        return f"{CONFIG.S3_RESTRICTED_CDN_ROOT.rstrip('/')}/{self.key}"

    @property
    def zip_file_identifier(self) -> str:
        """Parse identifier from key, raising an Exception if not a zip file extension."""
        base_name, extension = os.path.splitext(self.key)
        if extension.lower().strip() != ".zip":
            message = f"File does not have a '.zip' extension: {self.key}"
            raise ValueError(message)
        return base_name

    def validate_message(self) -> None:
        """Exercise important properties from ZipFileEventMessage to validate.

        If message is not valid, an exception will be logged to Sentry.
        """
        try:
            _ = self.modified
            _ = self.event
            _ = self.zip_file
            _ = self.zip_file_identifier
        except (ValueError, AttributeError, ParserError) as exc:
            message = f"Invalid SQS Message, reason: '{exc}', message: {self.raw}"
            logger.error(message)  # noqa: TRY400
            raise MessageValidationError(message) from exc


class SQSClient:
    """Class to manage Messages from queue containing zip file actions."""

    def __init__(self, queue_name: str, queue_url: str | None = None) -> None:
        self.queue_name = queue_name
        self._queue_url: str | None = queue_url

    @property
    def client(self) -> "SQSClientType":
        return boto3.client("sqs")

    @property
    def queue_url(self) -> str:
        """Property to provide QueueUrl, caching it for reuse."""
        if not self._queue_url:
            self._queue_url = self.get_queue_url()
        return self._queue_url

    def get_queue_url(self) -> str:
        """Get SQS queue URL from name."""
        return self.client.get_queue_url(QueueName=self.queue_name)["QueueUrl"]

    def get_message_count(self) -> int:
        response = self.client.get_queue_attributes(
            QueueUrl=self.queue_url, AttributeNames=["ApproximateNumberOfMessages"]
        )
        return int(response["Attributes"]["ApproximateNumberOfMessages"])

    def get_next_valid_message(
        self, wait_time: int | None = None
    ) -> ZipFileEventMessage | None:
        """Fetch next ZipFileEventMessage from queue of zip file actions.

        Before the ZipFileEventMessage is returned, it is first validated.  If it fails
        validation, an error is logged, and this method continues to return the next
        valid message.
        """
        response = self.client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=wait_time or 5,
        )
        messages = response.get("Messages", [])
        if messages:
            try:
                return ZipFileEventMessage(messages[0])
            except MessageValidationError:
                return self.get_next_valid_message(wait_time=wait_time)
        return None

    def get_valid_messages_iter(
        self, wait_time: int | None = None
    ) -> Iterator[ZipFileEventMessage]:
        """Iterator that yields all valid ZipFileEventMessages in queue"""
        while True:
            message = self.get_next_valid_message(wait_time=wait_time)
            if not message:
                break
            yield message

    def delete_message(self, receipt_handle: str) -> bool:
        """Delete single message from queue via receipt handle."""
        self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)
        return True
