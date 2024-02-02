"""harvester.harvest.ogm"""

import datetime
import hashlib
import logging
import os
import re
import shutil
import time
from collections.abc import Callable, Iterator

import pygit2  # type: ignore[import-untyped]
import smart_open  # type: ignore[import-untyped]
import yaml
from attrs import define, field

from harvester.config import Config
from harvester.harvest import Harvester
from harvester.harvest.exceptions import (
    OGMFilenameFilterMethodError,
    OGMFromDateExceedsEpochDateError,
)
from harvester.records import (
    FGDC,
    GBL1,
    ISO19139,
    Aardvark,
    Record,
    SourceRecord,
)
from harvester.utils import date_parser

logger = logging.getLogger(__name__)

CONFIG = Config()


@define
class OGMHarvester(Harvester):
    """Harvester of OpenGeoMetadata (OGM) GIS metadata records.

    OGM harvests, both full and incremental, rely on reading a configuration YAML file
    and then locally, temporarily cloning repositories from the OpenGeoMetadata Github
    organization.  The configuration YAML then configures what metadata formats and
    filename patterns can be used to scope files for harvest.

    The distinguishing feature of incremental harvests is using the git history to
    understand when and how files were modified, such that we can use a "from date" to
    isolate all changes on or after that date.

    Args:
        include_repositories: if set, list of repositories to include in harvest, else all
            repositories from config YAML are used
        exclude_repositories: if set, list of repositories to exclude in harvest
        remove_local_repos: if True, remove local clone of OGM repository after harvest
            - helpful for local testing/debugging where if set to False, this avoids the
            need to re-clone each run
    """

    include_repositories: list | None = field(default=None)
    exclude_repositories: list | None = field(default=None)
    remove_local_repos: bool = field(default=True)

    def full_harvest_get_source_records(self) -> Iterator[Record]:
        """Method to provide records for a full harvest."""
        return self._get_source_records(OGMRepository.get_all_records)

    def incremental_harvest_get_source_records(self) -> Iterator[Record]:
        """Method to provide records for an incremental harvest."""
        return self._get_source_records(
            OGMRepository.get_modified_records, self.from_date
        )

    def _get_source_records(
        self,
        retrieve_records_func: (
            Callable[["OGMRepository"], Iterator["OGMRecord"]]
            | Callable[["OGMRepository", str], Iterator["OGMRecord"]]
        ),
        *args: str,
    ) -> Iterator[Record]:
        """Common logic for both full and incremental OGM harvests.

        Full and incremental harvests share all logic except the OGMRepository method
        used to return the files in scope.  The appropriate args are passed to this
        method once identified.

        Args:
            retrieve_records_func: one of two possible methods from OGMRepository
                - get_current_records()
                - get_modified_records()
            args: for incremental harvests, included in args should be a from date string
        """
        repo_configs = self.get_repositories()

        for repo_name, repo_config in repo_configs.items():
            repo = OGMRepository(name=repo_name, config=repo_config)
            ogm_records_iterator = repo.filter_records(retrieve_records_func(repo, *args))

            for ogm_record in ogm_records_iterator:
                yield Record(
                    identifier=ogm_record.identifier,
                    source_record=self.create_source_record(
                        repo.metadata_format,
                        ogm_record.identifier,
                        ogm_record.harvest_event,
                        ogm_record.read(),
                    ),
                )

            if self.remove_local_repos:
                repo.delete_local_cloned_repository()

    def get_repositories(self) -> dict[str, dict]:
        """Read OGM configuration YAML and filter repositories for harvest."""
        repo_configs = OGMRepository.load_repositories_config()

        included_repositories = self.include_repositories or list(
            set(repo_configs.keys())
        )
        excluded_repositories = self.exclude_repositories or []

        return {
            repo_name: repo_config
            for repo_name, repo_config in repo_configs.items()
            if repo_name in included_repositories
            and repo_name not in excluded_repositories
        }

    def create_source_record(
        self,
        metadata_format: str,
        identifier: str,
        change_type: str,
        data: str | bytes,
    ) -> SourceRecord:
        """Create source record."""
        source_record_class = {
            "gbl1": GBL1,
            "aardvark": Aardvark,
            "fgdc": FGDC,
            "iso19139": ISO19139,
        }[metadata_format]
        return source_record_class(
            identifier=identifier,
            data=data,
            event=change_type,
            origin="ogm",
        )


@define
class OGMRepository:
    """Class to represent an OGM Github repository.

    This class provides functionality like cloning and removing the Github repository
    locally, reading Git history to determined files modified on or after a date, and
    filtering records based on a desired metadata format.
    """

    name: str = field()
    config: dict = field()

    @classmethod
    def load_repositories_config(cls) -> dict:
        with smart_open.open(CONFIG.ogm_config_filepath, "r") as file:
            return yaml.safe_load(file)

    @property
    def metadata_format(self) -> str:
        return self.config["metadata_format"]

    @property
    def clone_root_directory(self) -> str:
        """Root directory for cloning repositories."""
        return CONFIG.ogm_clone_root_dir

    @property
    def local_repository_directory(self) -> str:
        """Path of cloned repository."""
        return os.path.join(self.clone_root_directory, self.name)

    @property
    def git_repository(self) -> pygit2.Repository:
        """Return pygit2 repository instance."""
        return pygit2.Repository(self.local_repository_directory)

    def clone_repository(self) -> pygit2.Repository:
        """Locally clone repository for parsing and file reading."""
        if not os.path.exists(self.local_repository_directory):
            clone_start_time = time.time()
            clone_url = f"{CONFIG.ogm_clone_root_url}/{self.name}"
            message = f"Cloning repository to: {self.local_repository_directory}"
            logger.debug(message)
            local_repo = pygit2.clone_repository(
                clone_url, self.local_repository_directory
            )
            message = f"Clone successful: {time.time() - clone_start_time}s"
            logger.info(message)
        else:
            local_repo = pygit2.Repository(self.local_repository_directory)
            message = (
                f"Repository exists, skipping clone: {self.local_repository_directory}"
            )
            logger.info(message)
        return local_repo

    def delete_local_cloned_repository(self) -> None:
        """Remove locally cloned repository.

        This is called between repository clones and file parsing, to keep memory
        footprint low when multiple repositories in harvest.
        """
        message = f"Removing local clone: {self.local_repository_directory}"
        logger.debug(message)
        shutil.rmtree(self.local_repository_directory)

    def get_all_records(self) -> Iterator["OGMRecord"]:
        """Get all records from current state of the repository."""
        self.clone_repository()
        for root, _dirs, files in os.walk(self.local_repository_directory):
            for filename in files:
                filepath = os.path.join(root, filename)
                yield OGMRecord(
                    identifier=self.create_identifier_from_filename(filepath),
                    filename=filepath,
                    harvest_event="created",
                )

    def get_modified_records(self, from_date: str) -> Iterator["OGMRecord"]:
        """Get all modified files since a from date."""
        # get last commit BEFORE date
        target_commit = self._get_commit_before_date(from_date)
        if not target_commit:
            return []

        # get all modified files SINCE this commit
        changes = self._get_modified_files_since_commit(target_commit)

        # parse modified file tuples into OGMRecord instances
        for change_type, relative_filename in changes:
            filename = os.path.join(self.local_repository_directory, relative_filename)
            if change_type in ["A", "M", "C"]:
                event = "created"
            elif change_type in ["D"]:
                event = "deleted"
            else:
                message = f"Git file change type not handled: '{change_type}'"
                logger.error(message)
                continue
            yield OGMRecord(
                identifier=self.create_identifier_from_filename(filename),
                filename=filename,
                relative_filename=relative_filename,
                harvest_event=event,
                git_change_type=change_type,
                target_commit=target_commit,
            )

    def _get_commit_before_date(self, target_date: str) -> pygit2.Commit | None:
        """Identify last commit BEFORE a target date."""
        # raise exception if date is before epoch time of 1979-01-01
        if date_parser(target_date) < date_parser("1979-01-01"):
            raise OGMFromDateExceedsEpochDateError

        local_repo = self.clone_repository()

        from_timestamp = date_parser(target_date).timestamp()
        commits = [
            commit
            for commit in local_repo.walk(local_repo.head.target, pygit2.GIT_SORT_TIME)
            if commit.commit_time >= from_timestamp
        ]
        if not commits:
            message = f"Could not find any commits after date: {target_date}"
            logger.info(message)
            return None

        earliest_commit = commits[-1]
        if not earliest_commit.parents:
            target_commit = earliest_commit
        else:
            target_commit = commits[-1].parents[0]
        target_commit_date = datetime.datetime.fromtimestamp(
            target_commit.commit_time, tz=datetime.UTC
        ).isoformat()
        message = (
            f"Last commit before date '{target_date}': "
            f"{target_commit_date}, {target_commit.hex}"
        )
        logger.debug(message)
        return target_commit

    def _get_modified_files_since_commit(
        self, target_commit: pygit2.Commit
    ) -> list[tuple[str, str]]:
        """Get all modified files SINCE a target commit.

        This method returns a list of tuples indicating the git diff status character and
        filepath, e.g.: [("A", "files/file1.xml"), ("D", "files/file2.xml")], informing
        that file1.xml was added, and file2.xml was deleted.
        """
        deltas = self.git_repository.diff(target_commit.hex, "HEAD").deltas
        return [
            (delta.status_char(), delta.new_file.raw_path.decode()) for delta in deltas
        ]

    def filter_records(self, records: Iterator["OGMRecord"]) -> Iterator["OGMRecord"]:
        """Filter files to include in harvest based on strategy in repository config."""
        filter_regex = self._get_filter_regex()
        yield from [record for record in records if filter_regex.match(record.filename)]

    def _get_filter_regex(self) -> re.Pattern:
        """Identify file filter strategy for repository and provide regex expression.

        Each OGM configuration must include a strategy to filter what files from the
        repository are included in the harvest.  This method looks for particular keys
        in the configuration that are used as strategies.  The two currently supported
        are:
            - "filename_regex": apply a regular expression to all filepaths
            - "file_directory": recursively include all files under a given directory

        If none of these strategies are defined for the repository, throw an error as we
        cannot know which files to include (i.e. many repos contain the same record in
        multiple formats, or files that are not metadata records at all).
        """
        if "filename_regex" in self.config and "file_directory" in self.config:
            message = (
                "Both 'filename_regex' and 'file_directory' defined, only one "
                "file filter strategy allowed."
            )
            raise OGMFilenameFilterMethodError(message)

        if "filename_regex" in self.config:
            return re.compile(self.config["filename_regex"].removesuffix("\n"))

        if "file_directory" in self.config:
            return re.compile(rf".+?/{self.config['file_directory']}/.+?\.json")

        message = "File filtering method not found in repository config."
        raise OGMFilenameFilterMethodError(message)

    def create_identifier_from_filename(self, filename: str) -> str:
        """Generate TIMDEX identifier from OGM record filepath.

        For OGM harvests, we cannot know how external institutions are selecting and
        formatting identifiers for their records.  By minting an identifier based on the
        record's filepath in the repository, we have a guarantee that the identifier is
        compatible with TIMDEX, and will change anytime the OGM repository may change
        the layout of their files.
        """
        relative_filename = filename.removeprefix(self.local_repository_directory)
        return f"{self.name}:{hashlib.md5(relative_filename.encode()).hexdigest()[:12]}"  # noqa: S324


@define
class OGMRecord:
    """Class to represent a single record file from an OGM repository.

    This class is helpful to provide methods to read files either directly from disk (full
    harvests) or potentially from the git history (incremental harvests).
    """

    identifier: str = field()
    filename: str = field()
    relative_filename: str = field(default=None)
    harvest_event: str = field(default=None)
    git_change_type: str = field(default=None)
    target_commit: pygit2.Commit = field(default=None)

    _raw: bytes = field(default=None)

    def read(self) -> bytes:
        if not self._raw:
            self._raw = self._read_from_file_or_commit()
        return self._raw

    def _read_from_file_or_commit(self) -> bytes:
        if not os.path.exists(self.filename) and self.target_commit:
            return self._read_deleted_file_from_commit()
        return self._read_from_file()

    def _read_from_file(self) -> bytes:
        with open(self.filename, "rb") as f:
            return f.read()

    def _read_deleted_file_from_commit(self) -> bytes:
        return self.target_commit.tree[self.relative_filename].read_raw()
