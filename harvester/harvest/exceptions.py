class OGMFilenameFilterMethodError(Exception):
    """Raised when OGM harvest attempts to filter files via an unknown method."""


class OGMFromDateExceedsEpochDateError(Exception):
    """Only dates after 1979-01-01 are supported for OGM incremental harvests."""


class GithubApiRateLimitExceededError(Exception):
    """Raised when Github rate limit exceeded."""
