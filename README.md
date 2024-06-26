# geo-harvester

Geo-Harvester is a python CLI application for harvesting, normalizing, and writing GIS and geospatial metadata, with a focus on providing this metadata for TIMDEX.

At a high level, this is accomplished by:
1. fetching metadata records generated by MIT (S3) or from OpenGeoMetadata (OGM) repositories (Github)
2. normalize this metadata to the [Aardvark metadata format](https://opengeometadata.org/ogm-aardvark/)
3. based on normalized metadata, sending EventBridge events for other applications to potentially move and copy data

## Development

- To install with dev dependencies: `make install`
- To update dependencies: `make update`
- To run unit tests: `make test`
- To lint the repo: `make lint`
- To run the app: `pipenv run harvester --help`

## Environment Variables

### Required
```shell
SENTRY_DSN=### If set to a valid Sentry DSN, enables Sentry exception monitoring. This is not needed for local development.
WORKSPACE=### Set to `dev` for local development, this will be set to `stage` and `prod` in those environments by Terraform.
```

### Optional
```shell
S3_RESTRICTED_CDN_ROOT=### S3 bucket + prefix for CDN restricted, e.g. 's3://<bucket>/path/to/restricted'
S3_PUBLIC_CDN_ROOT=### S3 bucket + prefix for CDN public, e.g. 's3://<bucket>/path/to/public'
S3_TIMDEX_ALMA=### S3 bucket + prefix for previously extracted Alma source records, e.g. 's3://<timdex-extract-bucket>/alma'
GEOHARVESTER_SQS_TOPIC_NAME=### default value for CLI argument --sqs-topic-name
OGM_CONFIG_FILEPATH=### optional location for OGM configuration YAML
OGM_CLONE_ROOT_URL=### optional base URL or filepath for where to clone OGM repositories from
OGM_CLONE_ROOT_DIR=### optional location for where cloned repositories are saved locally
```

## CLI Commands

All CLI commands can be run with `pipenv run <COMMAND>`.

### `harvester`

```text
Usage: -c [OPTIONS] COMMAND [ARGS]...

  Root harvester command that other sub-commands extend.

Options:
  -v, --verbose  Pass to log at debug level instead of info.
  -h, --help     Show this message and exit.

Commands:
  harvest  Harvest command with sub-commands for different sources.
  ping     Debug ping/pong command
```

### `harvester ping`

```text
Usage: -c ping [OPTIONS]

  Debug ping/pong command.

  This command is purely for debugging purposes to ensure docker container
  and/or application is functional and responsive before any meaningful
  business logic.

Options:
  -h, --help  Show this message and exit.
```

### `harvester harvest`

Base command for harvests. Expecting sub-command `mit` or `ogm`.

```text
Usage: -c harvest [OPTIONS] COMMAND [ARGS]...

  Harvest command with sub-commands for different sources.

Options:
  -t, --harvest-type [full|incremental]
                                  Type of harvest, may be: 'incremental' or
                                  'full'.
  -f, --from-date TEXT            filter for files modified on or after this
                                  date; format YYYY-MM-DD.
  -u, --until-date TEXT           filter for files modified before this date;
                                  format YYYY-MM-DD.
  -o, --output-file TEXT          Filepath to write single, combined JSONLines
                                  file of normalized MITAardvark metadata for
                                  ALL harvested records.  This is the expected
                                  format for the TIMDEX pipeline.
  -h, --help                      Show this message and exit.

Commands:
  mit  Harvest and normalize MIT geospatial metadata records.
  ogm  Harvest and normalize OpenGeoMetadata (OGM) geospatial metadata...
```

### `harvester harvest mit`

```text
Usage: -c harvest mit [OPTIONS]

  Harvest and normalize MIT geospatial metadata records.

Options:
  -i, --input-files TEXT          Directory location of source record zip
                                  files (may be local or s3). Defaults to env
                                  var S3_RESTRICTED_CDN_ROOT if not set.
                                  [required]
  -osd, --output-source-directory TEXT
                                  Directory to write source metadata for EACH
                                  harvested record file with naming convention
                                  '<identifier>.<format>.source.xml|json'.
                                  Defaults to env var S3_PUBLIC_CDN_ROOT if
                                  not set.
  -ond, --output-normalized-directory TEXT
                                  Directory to write normalized MITAardvark
                                  metadata for EACH harvested record file with
                                  naming convention
                                  '<identifier>.aardvark.normalized.json'.
                                  Defaults to env var S3_PUBLIC_CDN_ROOT if
                                  not set.
  -s, --sqs-topic-name TEXT       SQS topic name with messages capturing zip
                                  file modifications. Defaults to env var
                                  GEOHARVESTER_SQS_TOPIC_NAME if not set.
                                  [required]
  --preserve-sqs-messages         If set, SQS messages will remain in the
                                  queue after incremental harvest.
  --skip-eventbridge-events       If set, will skip sending EventBridge events
                                  to manage files in CDN.
  -h, --help                      Show this message and exit.
```

### `harvester harvest ogm`

```text
Usage: -c harvest ogm [OPTIONS]

  Harvest and normalize OpenGeoMetadata (OGM) geospatial metadata records.

Options:
  --include-repositories TEXT  If set, limit to only these comma seperated
                               list of repositories for harvest.
  --exclude-repositories TEXT  If set, exclude these comma seperated list of
                               repositories from harvest.
  -h, --help                   Show this message and exit.
```
