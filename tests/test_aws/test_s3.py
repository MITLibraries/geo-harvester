import pytest
from botocore.exceptions import ClientError

from harvester.aws.s3 import S3Client
from harvester.aws.sqs import SQSClient


def test_s3client_list_empty(mocked_restricted_bucket_empty):
    assert len(S3Client.list_objects(mocked_restricted_bucket_empty, "")) == 0


def test_s3client_list_single(mocked_restricted_bucket_one_legacy_fgdc_zip):
    assert (
        len(S3Client.list_objects(mocked_restricted_bucket_one_legacy_fgdc_zip, "")) == 1
    )


def test_sqsclient_get_queue_url_success(mocked_sqs_topic_name, mock_boto3_sqs_client):
    mock_boto3_sqs_client.get_queue_url.return_value = {"QueueUrl": "http://example.com"}
    sqs_client = SQSClient(mocked_sqs_topic_name)
    assert sqs_client.get_queue_url() == "http://example.com"


def test_sqsclient_get_queue_url_fail(mock_boto3_sqs_client):
    error_response = {
        "Error": {
            "Code": "QueueDoesNotExist",
            "Message": "The queue does not exist",
        }
    }
    mock_boto3_sqs_client.get_queue_url.side_effect = ClientError(
        error_response, "get_queue_url"
    )
    sqs_client = SQSClient("bad-queue-does-not-exist")
    with pytest.raises(ClientError) as exc_info:
        sqs_client.get_queue_url()
    assert exc_info.value.response["Error"]["Code"] == "QueueDoesNotExist"


def test_sqsclient_get_message_count(mocked_sqs_topic_name, mock_boto3_sqs_client):
    message_count = 42
    mock_boto3_sqs_client.get_queue_url.return_value = {"QueueUrl": "http://example.com"}
    mock_boto3_sqs_client.get_queue_attributes.return_value = {
        "Attributes": {"ApproximateNumberOfMessages": message_count}
    }
    sqs_client = SQSClient(mocked_sqs_topic_name)
    assert sqs_client.get_message_count() == message_count
