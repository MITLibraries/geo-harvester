# ruff: noqa: PLR2004, SLF001, D205, D212

import os
import shutil
from unittest import mock

import git
import pytest

from harvester.config import Config
from harvester.harvest.exceptions import (
    OGMFilenameFilterMethodError,
    OGMFromDateExceedsEpochDateError,
)

CONFIG = Config()


@pytest.fixture(autouse=True)
def teardown_test_ogm_repos():
    """This fixture is automatically used and applied to all tests in this file.

    This fixture checks if one of the test repositories was cloned during the test, but
    not removed, and removes if so.
    """
    yield None
    for repo_dir in [
        f"{CONFIG.ogm_clone_root_dir}/edu.earth",
        f"{CONFIG.ogm_clone_root_dir}/edu.venus",
        f"{CONFIG.ogm_clone_root_dir}/edu.pluto",
    ]:
        if os.path.exists(repo_dir):
            shutil.rmtree(repo_dir)


def test_ogm_repository_properties(ogm_repository_earth):
    assert ogm_repository_earth.metadata_format == "gbl1"
    assert ogm_repository_earth.clone_root_directory == "output/ogm"
    assert ogm_repository_earth.local_repository_directory == "output/ogm/edu.earth"


def test_ogm_repository_clone_success(caplog, ogm_repository_earth):
    caplog.set_level("DEBUG")
    local_repo = ogm_repository_earth.clone_repository()
    assert isinstance(local_repo, git.repo.base.Repo)
    assert os.path.exists(ogm_repository_earth.local_repository_directory)

    ogm_repository_earth.clone_repository()
    assert "Repository exists" in caplog.text


def test_ogm_repository_clone_remove_success(ogm_repository_earth):
    ogm_repository_earth.clone_repository()
    ogm_repository_earth.delete_local_clone()
    assert not os.path.exists(ogm_repository_earth.local_repository_directory)


def test_ogm_repository_mint_identifier(ogm_repository_earth):
    identifier = ogm_repository_earth.create_identifier_from_filename("a/b/c/d.json")
    assert identifier == "edu.earth:a89f32f7664c"


def test_ogm_repository_get_current_records(ogm_repository_earth):
    # NOTE: the files/records returned are pre-filtering from config YAML
    expected_identifiers = [
        "edu.earth:5f5ac295b365",  # record1.json
        "edu.earth:3072f18cdeb5",  # record2.json
    ]
    records = list(ogm_repository_earth.get_current_records())
    assert len(records) > 2
    assert (
        len([record for record in records if record.identifier in expected_identifiers])
        == 2
    )


def test_ogm_repository_filter_records_regex_success(ogm_repository_earth):
    records_iterator = ogm_repository_earth.get_current_records()
    filtered_records = list(ogm_repository_earth.filter_records(records_iterator))
    assert len(filtered_records) == 2


def test_ogm_repository_filter_records_directory_success(ogm_repository_venus):
    records_iterator = ogm_repository_venus.get_current_records()
    filtered_records = list(ogm_repository_venus.filter_records(records_iterator))
    assert len(filtered_records) == 2


def test_ogm_repository_filter_records_bad_method_error(ogm_repository_earth):
    ogm_repository_earth.config.pop("filename_regex")
    ogm_repository_earth.config["filename_rabbit_out_of_hat"] = "rabbit.json"
    records_iterator = ogm_repository_earth.get_current_records()
    with pytest.raises(
        OGMFilenameFilterMethodError,
        match="File filtering method not found in repository config.",
    ):
        list(ogm_repository_earth.filter_records(records_iterator))


def test_ogm_repository_acknowledge_epoch_limit_to_from_date(ogm_repository_earth):
    with pytest.raises(OGMFromDateExceedsEpochDateError):
        ogm_repository_earth._get_commit_before_date("1920-01-01")


def test_ogm_repository_early_date_gets_first_commit(ogm_repository_earth):
    commit = ogm_repository_earth._get_commit_before_date("1980-01-01")
    assert commit.hexsha == "b3c278aa8fbe5e97775ec6ec8e0f51893a151b16"
    assert commit.message == "first commit\n"


def test_ogm_repository_date_after_first_record_gets_first_record_commit(
    ogm_repository_earth,
):
    commit = ogm_repository_earth._get_commit_before_date("2005-01-01")
    assert commit.hexsha == "e17c899b753d0fa17a90bc56f6cb1367b23b5f62"
    assert commit.message == "First file commit\n"


def test_ogm_repository_date_after_all_commits_returns_none(caplog, ogm_repository_earth):
    assert ogm_repository_earth._get_commit_before_date("2015-01-01") is None
    assert "Could not find any commits after date: 2015-01-01" in caplog.text


def test_ogm_repository_get_two_added_files_since_root_commit(ogm_repository_earth):
    root_commit = ogm_repository_earth._get_commit_before_date("1980-01-01")
    file_list = ogm_repository_earth._get_modified_files_since_commit(root_commit)
    assert file_list == [["A", "gbl1/record1.json"], ["A", "gbl1/record2.json"]]


def test_ogm_repository_get_deleted_and_added_file(ogm_repository_pluto):
    """
    commit 5355b82c13fbd6c709dc5aea3501dbe3b74e8810
    Date:   Wed Jan 1 00:00:00 2020 -0500
        Removed second file and add third

    <----- REQUESTED DATE HERE ----->

    commit 21d554558c69711a8f671ed5400a48a54ab2b9cf
    Date:   Fri Jan 1 00:00:00 2010 -0500
        Second file commit
    """
    root_commit = ogm_repository_pluto._get_commit_before_date("2015-01-01")
    file_list = ogm_repository_pluto._get_modified_files_since_commit(root_commit)
    assert file_list == [["D", "fgdc/record2.xml"], ["A", "fgdc/record3.xml"]]


def test_ogm_repository_date_before_all_records_gets_all_records(ogm_repository_earth):
    records = list(ogm_repository_earth.get_modified_records("1999-01-01"))
    assert len(records) == 2


def test_ogm_repository_date_after_first_record_gets_second_record(ogm_repository_earth):
    records = list(ogm_repository_earth.get_modified_records("2005-01-01"))
    assert len(records) == 1
    record = records[0]
    assert record.filename.endswith("record2.json")


def test_ogm_repository_deleted_file_sets_deleted_harvest_event(ogm_repository_pluto):
    records = list(ogm_repository_pluto.get_modified_records("2015-01-01"))
    assert len(records) == 2
    assert ["D", "A"] == [record.git_change_type for record in records]
    assert ["deleted", "created"] == [record.harvest_event for record in records]


def test_ogm_repository_unhandled_git_file_action_logged_and_skipped(
    caplog,
    ogm_repository_earth,
):
    with mock.patch(
        "harvester.harvest.ogm.OGMRepository._get_modified_files_since_commit"
    ) as mock_files_since_commit:
        mock_files_since_commit.return_value = [
            ["X", "gbl1/record1.json"],
            ["A", "gbl1/record2.json"],
        ]
        records = list(ogm_repository_earth.get_modified_records("1999-01-01"))
    assert len(records) == 1
    assert "Git file change type not handled: 'X'" in caplog.text


def test_ogm_repository_recent_date_returns_no_files(ogm_repository_earth):
    records = list(ogm_repository_earth.get_modified_records("2024-01-01"))
    assert records == []


def test_ogm_record_read_from_file(ogm_record_from_disk):
    with open(ogm_record_from_disk.filename, "rb") as f:
        assert f.read() == ogm_record_from_disk.read()


def test_ogm_record_read_from_git_history(ogm_record_from_git_history):
    """This test confirms that a deleted file can still be read from git history."""
    assert ogm_record_from_git_history.harvest_event == "deleted"
    assert ogm_record_from_git_history.filename == "output/ogm/edu.pluto/fgdc/record2.xml"
    assert not os.path.exists(ogm_record_from_git_history.filename)
    assert ogm_record_from_git_history.read().startswith(
        b'<?xml version="1.0" encoding="utf-8" ?><!DOCTYPE metadata SYSTEM '
        b'"http://www.fgdc.gov/metadata/fgdc-std-001-1998.dtd"><metadata>'
    )


def test_ogm_harvester_get_full_source_records(ogm_full_harvester, ogm_full_record_set):
    records = list(ogm_full_harvester.get_source_records())
    assert len(records) == 6
    assert {record.identifier for record in records} == ogm_full_record_set


def test_ogm_harvester_get_incremental_source_records_early_date(
    ogm_incremental_harvester, ogm_full_record_set
):
    ogm_incremental_harvester.from_date = "1995-01-01"
    records = list(ogm_incremental_harvester.get_source_records())
    assert len(records) == 6
    assert {record.identifier for record in records} == ogm_full_record_set


def test_ogm_harvester_incremental_single_repo_and_recent_date_get_deleted_and_created(
    ogm_incremental_harvester,
):
    ogm_incremental_harvester.from_date = "2015-01-01"
    ogm_incremental_harvester.include_repositories = ["edu.pluto"]
    records = list(ogm_incremental_harvester.get_source_records())
    assert {record.identifier for record in records} == {
        "edu.pluto:287fedb362ea",
        "edu.pluto:83fd37f6a879",
    }
    assert {record.source_record.event for record in records} == {
        "deleted",
        "created",
    }
