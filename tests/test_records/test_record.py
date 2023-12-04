import pytest

from harvester.records import DeletedSourceRecord


def test_deleted_record_normalize_raise_error():
    record = DeletedSourceRecord()
    with pytest.raises(RuntimeError):
        record.normalize()
