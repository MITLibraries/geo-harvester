"""harvester.aws.sqs"""

import logging

import boto3
from mypy_boto3_sqs.client import SQSClient as SQSClientType
from mypy_boto3_sqs.service_resource import Message

logger = logging.getLogger(__name__)


class SQSClient:
    def __init__(self, queue_name: str) -> None:
        self.queue_name = queue_name

    @classmethod
    def get_client(cls) -> SQSClientType:
        return boto3.client("sqs")

    def get_queue_url(self) -> str:
        """Get SQS queue URL from name."""
        client = self.get_client()
        return client.get_queue_url(QueueName=self.queue_name)["QueueUrl"]

    def get_message_count(self) -> int:
        client = self.get_client()
        queue_url = self.get_queue_url()
        response = client.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
        )
        return int(response["Attributes"]["ApproximateNumberOfMessages"])

    def fetch_messages(self) -> list[Message]:  # pragma: nocover
        return []

    def delete_message(self) -> bool:  # pragma: nocover
        return False
