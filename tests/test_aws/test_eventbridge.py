from unittest.mock import patch

from harvester.aws.eventbridge import EventBridgeClient


def test_eventbridge_client_get_client_success(mock_eventbridge_client):
    assert EventBridgeClient.get_client() == mock_eventbridge_client


def test_eventbridge_client_put_events_success(mock_eventbridge_client):
    payload = {"msg": "in a bottle"}
    with patch.object(mock_eventbridge_client, "put_events") as mocked_put:
        EventBridgeClient.send_event(detail=payload)
    mocked_put.assert_called_with(
        Entries=[
            {
                "Detail": '{"msg": "in a bottle"}',  # JSON serialized
                "DetailType": "geo-harvester run",  # default values
                "Source": "geo-harvester.app",
                "EventBusName": "default",
            }
        ]
    )
