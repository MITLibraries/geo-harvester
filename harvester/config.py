import logging
import os
from typing import Any

import sentry_sdk

logger = logging.getLogger(__name__)


class Config:
    REQUIRED_ENV_VARS = (
        "WORKSPACE",
        "SENTRY_DSN",
        "S3_RESTRICTED_CDN_ROOT",
        "S3_PUBLIC_CDN_ROOT",
    )
    OPTIONAL_ENV_VARS = (
        "GEOHARVESTER_SQS_TOPIC_NAME",
        "OGM_CONFIG_FILEPATH",
        "OGM_CLONE_ROOT_URL",
        "OGM_CLONE_ROOT_DIR",
        "GITHUB_API_TOKEN",
    )

    def check_required_env_vars(self) -> None:
        """Method to raise exception if required env vars not set."""
        missing_vars = [var for var in self.REQUIRED_ENV_VARS if not os.getenv(var)]
        if missing_vars:
            message = f"Missing required environment variables: {', '.join(missing_vars)}"
            raise OSError(message)

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401
        """Provide dot notation access to configurations and env vars on this class."""
        if name in self.REQUIRED_ENV_VARS or name in self.OPTIONAL_ENV_VARS:
            return os.getenv(name)
        message = f"'{name}' not a valid configuration variable"
        raise AttributeError(message)

    @property
    def http_cdn_root(self) -> str:
        """Property to return the base HTTP CDN URL path based on environment."""
        return {
            None: "https://cdn.dev1.mitlibrary.net/geo",
            "test": "https://cdn.dev1.mitlibrary.net/geo",
            "dev": "https://cdn.dev1.mitlibrary.net/geo",
            "stage": "https://cdn.stage.mitlibrary.net/geo",
            "prod": "https://cdn.libraries.mit.edu/geo",
        }[self.WORKSPACE]

    @property
    def ogm_config_filepath(self) -> str:
        return os.getenv("OGM_CONFIG_FILEPATH", "harvester/ogm_repositories_config.yaml")

    @property
    def ogm_clone_root_url(self) -> str:
        return os.getenv("OGM_CLONE_ROOT_URL", "https://github.com/OpenGeoMetadata")

    @property
    def ogm_clone_root_dir(self) -> str:
        return os.getenv("OGM_CLONE_ROOT_DIR", "output/ogm")

    @property
    def github_api_base_url(self) -> str:
        return "https://api.github.com/repos/OpenGeoMetadata"  # pragma: no cover

    @property
    def github_api_token(self) -> str:
        return self.GITHUB_API_TOKEN


def configure_logger(
    logger: logging.Logger,
    verbose: bool,  # noqa: FBT001
) -> str:
    if verbose:
        logging.basicConfig(
            format="%(asctime)s %(levelname)s %(name)s.%(funcName)s() line %(lineno)d: "
            "%(message)s"
        )
        logger.setLevel(logging.DEBUG)
        for handler in logging.root.handlers:
            handler.addFilter(logging.Filter("harvester"))
    else:
        logging.basicConfig(
            format="%(asctime)s %(levelname)s %(name)s.%(funcName)s(): %(message)s"
        )
        logger.setLevel(logging.INFO)
    return (
        f"Logger '{logger.name}' configured with level="
        f"{logging.getLevelName(logger.getEffectiveLevel())}"
    )


def configure_sentry() -> str:
    env = os.getenv("WORKSPACE")
    sentry_dsn = os.getenv("SENTRY_DSN")
    if sentry_dsn and sentry_dsn.lower() != "none":
        sentry_sdk.init(sentry_dsn, environment=env)
        return f"Sentry DSN found, exceptions will be sent to Sentry with env={env}"
    return "No Sentry DSN found, exceptions will not be sent to Sentry"
