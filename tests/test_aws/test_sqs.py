import json

import pytest
from botocore.exceptions import ClientError

from harvester.aws.sqs import MessageValidationError, SQSClient, ZipFileEventMessage


def test_sqsclient_get_queue_url_success(mocked_sqs_topic_name, mock_boto3_sqs_client):
    mock_boto3_sqs_client.get_queue_url.return_value = {"QueueUrl": "http://example.com"}
    sqs_client = SQSClient(mocked_sqs_topic_name)
    assert sqs_client.get_queue_url() == "http://example.com"


def test_sqsclient_get_queue_url_raise_error(mock_boto3_sqs_client):
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


def test_sqsclient_get_message_count_success(
    mocked_sqs_topic_name, mock_boto3_sqs_client
):
    message_count = 42
    mock_boto3_sqs_client.get_queue_url.return_value = {"QueueUrl": "http://example.com"}
    mock_boto3_sqs_client.get_queue_attributes.return_value = {
        "Attributes": {"ApproximateNumberOfMessages": message_count}
    }
    sqs_client = SQSClient(mocked_sqs_topic_name)
    assert sqs_client.get_message_count() == message_count


def test_valid_file_event_message_deleted_init_success(
    valid_sqs_message_deleted_dict, valid_sqs_message_deleted_instance
):
    message = ZipFileEventMessage(valid_sqs_message_deleted_dict)
    for prop in [
        "message_id",
        "receipt_handle",
        "body",
        "event",
        "reason",
        "bucket",
        "modified",
        "zip_file",
        "zip_file_identifier",
    ]:
        assert getattr(message, prop) == getattr(valid_sqs_message_deleted_instance, prop)


def test_valid_file_event_message_created_init_success(
    valid_sqs_message_created_dict, valid_sqs_message_created_instance
):
    message = ZipFileEventMessage(valid_sqs_message_created_dict)
    for prop in [
        "message_id",
        "receipt_handle",
        "body",
        "event",
        "reason",
        "bucket",
        "modified",
        "zip_file",
        "zip_file_identifier",
    ]:
        assert getattr(message, prop) == getattr(valid_sqs_message_created_instance, prop)


def test_invalid_zip_file_event_message_bad_filetype_raise_error(
    invalid_sqs_message_dict,
):
    with pytest.raises(
        MessageValidationError,
        match="Invalid SQS Message, reason: 'File does not have a '.zip' extension: "
        "testfile.txt'",
    ):
        _ = ZipFileEventMessage(invalid_sqs_message_dict)


def test_invalid_file_event_message_bad_event_raise_error(
    valid_sqs_message_deleted_instance,
):
    message = valid_sqs_message_deleted_instance
    body = json.loads(message.raw["Body"])
    body["detail-type"] = "Bad Object Action"
    message.raw["Body"] = json.dumps(body)
    with pytest.raises(
        MessageValidationError,
        match="Message detail-type not recognized: Bad Object Action",
    ):
        message.validate_message()


def test_valid_file_event_message_missing_env_vars_raise_error(
    monkeypatch,
    valid_sqs_message_deleted_instance,
):
    monkeypatch.delenv("S3_RESTRICTED_CDN_ROOT")
    with pytest.raises(
        MessageValidationError,
        match="Cannot determine CDN:Restricted path without env var "
        "S3_RESTRICTED_CDN_ROOT set",
    ):
        valid_sqs_message_deleted_instance.validate_message()


def test_sqsclient_get_next_valid_message_return_message_success(
    mocked_sqs_topic_name, mock_boto3_sqs_client, valid_sqs_message_deleted_dict
):
    mock_boto3_sqs_client.receive_message.return_value = {
        "Messages": [valid_sqs_message_deleted_dict]
    }
    sqs_client = SQSClient(mocked_sqs_topic_name)
    message = sqs_client.get_next_valid_message()
    assert isinstance(message, ZipFileEventMessage)


def test_sqsclient_get_next_valid_message_return_none_success(
    caplog,
    mocked_sqs_topic_name,
    mock_boto3_sqs_client,
    invalid_sqs_message_dict,
    valid_sqs_message_deleted_dict,
):
    mock_boto3_sqs_client.receive_message.side_effect = [
        {},
    ]
    sqs_client = SQSClient(mocked_sqs_topic_name)
    message = sqs_client.get_next_valid_message()
    assert message is None


def test_sqsclient_get_next_valid_message_handle_validation_error_success(
    caplog,
    mocked_sqs_topic_name,
    mock_boto3_sqs_client,
    invalid_sqs_message_dict,
    valid_sqs_message_deleted_dict,
):
    mock_boto3_sqs_client.receive_message.side_effect = [
        {"Messages": [invalid_sqs_message_dict]},
        {"Messages": [valid_sqs_message_deleted_dict]},
        {},
    ]
    sqs_client = SQSClient(mocked_sqs_topic_name)
    message = sqs_client.get_next_valid_message()
    # assert a valid one is retrieved after an invalid on skipped
    assert message is not None
    # assert method was called twice in total
    assert mock_boto3_sqs_client.receive_message.call_count == 2  # noqa: PLR2004
    # assert error was logged
    assert "Invalid SQS Message" in caplog.text


def test_sqsclient_get_valid_messages_iter_skip_and_yield_success(
    caplog,
    mocked_sqs_topic_name,
    mock_boto3_sqs_client,
    invalid_sqs_message_dict,
    valid_sqs_message_deleted_dict,
):
    mock_boto3_sqs_client.receive_message.side_effect = [
        {"Messages": [invalid_sqs_message_dict]},
        {"Messages": [valid_sqs_message_deleted_dict]},
        {},
    ]
    sqs_client = SQSClient(mocked_sqs_topic_name)
    messages = list(sqs_client.get_valid_messages_iter())
    assert len(messages) == 2  # noqa: PLR2004
    assert "Invalid SQS Message" in caplog.text


def test_sqsclient_get_valid_messages_skip_refetching_success(
    caplog,
    mocked_sqs_topic_name,
    mock_boto3_sqs_client,    
    valid_sqs_message_created_dict,
):
    caplog.set_level("DEBUG")
    # two messages with the same Message.MessageId
    mock_boto3_sqs_client.receive_message.side_effect = [
        {"Messages": [valid_sqs_message_created_dict]},
        {"Messages": [valid_sqs_message_created_dict]},
        {},
    ]
    sqs_client = SQSClient(mocked_sqs_topic_name)
    messages = list(sqs_client.get_valid_messages_iter())
    # ruff: noqa: PLR2004
    assert len(messages) == 1
    assert (
        "Skipping Message '81f4ce84-b18e-4c1f-8809-3b6fb69a25b5', already seen this "
        "harvest." in caplog.text
    )


def test_sqsclient_delete_message_success(
    mocked_sqs_topic_name, mock_boto3_sqs_client, valid_sqs_message_deleted_instance
):
    mock_boto3_sqs_client.receive_message.delete_message = None
    sqs_client = SQSClient(mocked_sqs_topic_name)
    sqs_client.delete_message(valid_sqs_message_deleted_instance.receipt_handle)
