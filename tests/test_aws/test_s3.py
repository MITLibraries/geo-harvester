from harvester.aws.s3 import S3Client


def test_s3client_list_empty_success(mocked_restricted_bucket_empty):
    assert len(S3Client.list_objects(mocked_restricted_bucket_empty, "")) == 0


def test_s3client_list_single_success(mocked_restricted_bucket_one_legacy_fgdc_zip):
    assert (
        len(S3Client.list_objects(mocked_restricted_bucket_one_legacy_fgdc_zip, "")) == 1
    )
