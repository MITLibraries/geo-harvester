import logging
from datetime import timedelta
from time import perf_counter

import click

from harvester.config import configure_logger, configure_sentry

logger = logging.getLogger(__name__)


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
    logger.info("Running process")


@main.command()
@click.pass_context
def ping(ctx: click.Context) -> None:
    """Debug ping/pong command"""
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
    type=str,
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
@click.pass_context
def harvest(
    ctx: click.Context,
    harvest_type: str,
    from_date: str,
    until_date: str,
) -> None:
    """Harvest command with sub-commands for different sources."""
    ctx.obj["HARVEST_TYPE"] = from_date
    ctx.obj["FROM_DATE"] = from_date
    ctx.obj["UNTIL_DATE"] = until_date


# Attach harvest group to main command
main.add_command(harvest)


@harvest.command(name="mit")
@click.option(
    "-i",
    "--input-files",
    required=True,
    envvar="GEO_INPUT_FILES",
    type=str,
    help="Directory location of source zip files (may be local or s3).",
)
@click.option(
    "-s",
    "--sqs-topic-arn",
    required=False,
    envvar="GEO_SQS_TOPIC_ARN",
    type=str,
    help="SQS Topic ARN with messages capturing zip file modifications.  Required when "
    "--harvest-type=incremental.",
)
@click.pass_context
def harvest_mit(
    ctx: click.Context,
    input_files: str,
    sqs_topic_arn: str,
) -> None:
    """Harvest and normalize MIT geospatial metadata records.

    NOTE: relies on 'harvest' command group arguments
    """

    # ensure SQS Topic ARN defined for incremental harvests
    if ctx["HARVEST_TYPE"] == "incremental" and not sqs_topic_arn:
        raise click.MissingParameter(
            "--sqs-topic-arn must be set when --harvest-type=incremental"
        )

    logger.info(
        "Total elapsed: %s",
        str(
            timedelta(seconds=perf_counter() - ctx.obj["START_TIME"]),
        ),
    )
    raise NotImplementedError()


@harvest.command(name="ogm")
@click.option(
    "--config-yaml-file",
    required=True,
    type=str,
    help="Filepath of config YAML that defines how to harvest from OGM.",
)
@click.pass_context
def harvest_ogm(
    ctx: click.Context,
    config_yaml_file: str,
) -> None:
    """Harvest and normalize OpenGeoMetadata (OGM) geospatial metadata records.

    NOTE: relies on 'harvest' command group arguments
    """
    logger.info(
        "Total elapsed: %s",
        str(
            timedelta(seconds=perf_counter() - ctx.obj["START_TIME"]),
        ),
    )
    raise NotImplementedError()
