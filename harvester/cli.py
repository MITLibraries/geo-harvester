# ruff: noqa: FBT001

import logging
from datetime import timedelta
from time import perf_counter

import click

from harvester.config import Config, configure_logger, configure_sentry
from harvester.harvest.alma import MITAlmaHarvester
from harvester.harvest.mit import MITHarvester
from harvester.harvest.ogm import OGMHarvester

logger = logging.getLogger(__name__)

CONFIG = Config()


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Pass to log at debug level instead of info.",
)
@click.pass_context
def main(ctx: click.Context, verbose: bool) -> None:
    """Root harvester command that other sub-commands extend."""
    ctx.ensure_object(dict)
    ctx.obj["START_TIME"] = perf_counter()
    root_logger = logging.getLogger()
    logger.info(configure_logger(root_logger, verbose))
    logger.info(configure_sentry())
    CONFIG.check_required_env_vars()
    logger.info("Running process")


@main.command()
@click.pass_context
def ping(ctx: click.Context) -> None:
    """Debug ping/pong command.

    This command is purely for debugging purposes to ensure docker container and/or
    application is functional and responsive before any meaningful business logic.
    """
    logger.debug("got ping, preparing to pong")
    click.echo("pong")
    logger.info(
        "Total elapsed: %s",
        str(
            timedelta(seconds=perf_counter() - ctx.obj["START_TIME"]),
        ),
    )


@click.group()
@click.option(
    "-t",
    "--harvest-type",
    required=False,
    type=click.Choice(["full", "incremental"], case_sensitive=False),
    default="incremental",
    help="Type of harvest, may be: 'incremental' or 'full'.",
)
@click.option(
    "-f",
    "--from-date",
    required=False,
    default=None,
    type=str,
    help="filter for files modified on or after this date; format YYYY-MM-DD.",
)
@click.option(
    "-u",
    "--until-date",
    required=False,
    default=None,
    type=str,
    help="filter for files modified before this date; format YYYY-MM-DD.",
)
@click.option(
    "-o",
    "--output-file",
    required=False,
    type=str,
    help="Filepath to write single, combined JSONLines file of normalized MITAardvark "
    "metadata for ALL harvested records.  This is the expected format for the "
    "TIMDEX pipeline.",
)
@click.pass_context
def harvest(
    ctx: click.Context,
    harvest_type: str,
    from_date: str,
    until_date: str,
    output_file: str,
) -> None:
    """Harvest command with sub-commands for different sources."""
    ctx.obj["HARVEST_TYPE"] = harvest_type
    ctx.obj["FROM_DATE"] = from_date
    ctx.obj["UNTIL_DATE"] = until_date
    ctx.obj["OUTPUT_FILE"] = output_file


# Attach harvest group to main command
main.add_command(harvest)


@harvest.command(name="mit")
@click.option(
    "-i",
    "--input-files",
    required=True,
    envvar="S3_RESTRICTED_CDN_ROOT",
    type=str,
    help="Directory location of source record zip files (may be local or s3). Defaults to"
    " env var S3_RESTRICTED_CDN_ROOT if not set.",
)
@click.option(
    "-osd",
    "--output-source-directory",
    required=False,
    envvar="S3_PUBLIC_CDN_ROOT",
    type=str,
    help="Directory to write source metadata for EACH harvested record file with naming "
    "convention '<identifier>.<format>.source.xml|json'. Defaults to env var "
    "S3_PUBLIC_CDN_ROOT if not set.",
)
@click.option(
    "-ond",
    "--output-normalized-directory",
    required=False,
    envvar="S3_PUBLIC_CDN_ROOT",
    type=str,
    help="Directory to write normalized MITAardvark metadata for EACH harvested record "
    "file with naming convention '<identifier>.aardvark.normalized.json'. Defaults "
    "to env var S3_PUBLIC_CDN_ROOT if not set.",
)
@click.option(
    "-s",
    "--sqs-topic-name",
    required=True,
    envvar="GEOHARVESTER_SQS_TOPIC_NAME",
    type=str,
    help="SQS topic name with messages capturing zip file modifications. Defaults to"
    " env var GEOHARVESTER_SQS_TOPIC_NAME if not set.",
)
@click.option(
    "--preserve-sqs-messages",
    required=False,
    is_flag=True,
    help="If set, SQS messages will remain in the queue after incremental harvest.",
)
@click.option(
    "--skip-eventbridge-events",
    required=False,
    is_flag=True,
    help="If set, will skip sending EventBridge events to manage files in CDN.",
)
@click.pass_context
def harvest_mit(
    ctx: click.Context,
    input_files: str,
    output_source_directory: str,
    output_normalized_directory: str,
    sqs_topic_name: str,
    preserve_sqs_messages: bool,
    skip_eventbridge_events: bool,
) -> None:
    """Harvest and normalize MIT geospatial metadata records."""
    harvester = MITHarvester(
        harvest_type=ctx.obj["HARVEST_TYPE"],
        from_date=ctx.obj["FROM_DATE"],
        until_date=ctx.obj["UNTIL_DATE"],
        input_files=input_files,
        sqs_topic_name=sqs_topic_name,
        preserve_sqs_messages=preserve_sqs_messages,
        skip_eventbridge_events=skip_eventbridge_events,
        output_source_directory=output_source_directory,
        output_normalized_directory=output_normalized_directory,
        output_file=ctx.obj["OUTPUT_FILE"],
    )
    results = harvester.harvest()
    logger.info(results)

    logger.info(  # pragma: no cover
        "Total elapsed: %s",
        str(
            timedelta(seconds=perf_counter() - ctx.obj["START_TIME"]),
        ),
    )


@harvest.command(name="ogm")
@click.option(
    "--include-repositories",
    required=False,
    type=str,
    help="If set, limit to only these comma seperated list of repositories for harvest.",
)
@click.option(
    "--exclude-repositories",
    required=False,
    type=str,
    help="If set, exclude these comma seperated list of repositories from harvest.",
)
@click.pass_context
def harvest_ogm(
    ctx: click.Context,
    include_repositories: str,
    exclude_repositories: str,
) -> None:  # pragma: no cover
    """Harvest and normalize OpenGeoMetadata (OGM) geospatial metadata records."""
    include_list = exclude_list = None
    if include_repositories:
        include_list = [repo.strip() for repo in include_repositories.split(",")]
    if exclude_repositories:
        exclude_list = [repo.strip() for repo in exclude_repositories.split(",")]

    harvester = OGMHarvester(
        harvest_type=ctx.obj["HARVEST_TYPE"],
        from_date=ctx.obj["FROM_DATE"],
        include_repositories=include_list,
        exclude_repositories=exclude_list,
        output_file=ctx.obj["OUTPUT_FILE"],
    )

    results = harvester.harvest()
    logger.info(results)

    logger.info(  # pragma: no cover
        "Total elapsed: %s",
        str(
            timedelta(seconds=perf_counter() - ctx.obj["START_TIME"]),
        ),
    )


@harvest.command(name="alma")
@click.option(
    "-i",
    "--input-files",
    required=True,
    envvar="S3_TIMDEX_ALMA",
    type=str,
    help=(
        "Directory location of source Alma MARC records, where XML files expected in form"
        " alma-<YYYY-MM-DD>-<HARVEST_TYPE>-extracted-records-to-index_##.xml."
    ),
)
@click.pass_context
def harvest_alma(ctx: click.Context, input_files: str) -> None:
    harvester = MITAlmaHarvester(
        harvest_type=ctx.obj["HARVEST_TYPE"],
        input_files=input_files,
        from_date=ctx.obj["FROM_DATE"],
        until_date=ctx.obj["UNTIL_DATE"],
        output_file=ctx.obj["OUTPUT_FILE"],
    )

    results = harvester.harvest()
    logger.info(results)

    logger.info(  # pragma: no cover
        "Total elapsed: %s",
        str(
            timedelta(seconds=perf_counter() - ctx.obj["START_TIME"]),
        ),
    )
