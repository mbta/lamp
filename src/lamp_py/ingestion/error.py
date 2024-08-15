class GTFSIngestException(Exception):
    """
    Generic exception for the py gtfs_rt_ingestion library
    """


class ConfigTypeFromFilenameException(GTFSIngestException):
    """
    Unable to derrive config type from a filename
    """

    def __init__(self, filename: str):
        message = f"Unable to deduce Configuration Type from {filename}"
        super().__init__(message)
        self.filename = filename


class ArgumentException(GTFSIngestException):
    """
    General Error to throw when incoming events are malformed
    """


class NoImplException(GTFSIngestException):
    """
    General Error for things LAMP hasn't implemented yet
    """


class IgnoreIngestion(GTFSIngestException):
    """
    General Error for files GTFS Ingestion should ignore
    """


class AWSException(GTFSIngestException):
    """
    General Error for raising with any AWS errors encountered.
    """
