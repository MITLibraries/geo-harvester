class AlmaCannotIdentifyLatestFullRunDateError(Exception):
    """Raised when latest full run date cannot be identified for Alma full harvests."""


class OGMFilenameFilterMethodError(Exception):
    """Raised when OGM harvest attempts to filter files via an unknown method."""


class OGMFromDateExceedsEpochDateError(Exception):
    """Only dates after 1979-01-01 are supported for OGM incremental harvests."""
