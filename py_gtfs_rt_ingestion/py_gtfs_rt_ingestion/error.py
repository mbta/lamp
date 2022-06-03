class GTFSIngestException(Exception):
    """
    Generic exception for the py gtfs_rt_ingestion library
    """
    pass

class ConfigTypeFromFilenameException(GTFSIngestException):
    """
    Unable to derrive config type from a filename
    """
    def __init__(self, filename):
        self.filename = filename
        self.message = \
            "Unable to deduce Configuration Type from filename %s" % filename

class ArgumentException(GTFSIngestException):
    pass

class NoImplException(GTFSIngestException):
    """
    General Error for things we haven't done yet.
    """
    pass

class AWSException(GTFSIngestException):
    """
    Error to throw when something goes wrong interacting with s3 or lambda
    """
    pass
