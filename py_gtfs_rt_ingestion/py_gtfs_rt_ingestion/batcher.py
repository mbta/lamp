from collections.abc import Iterable
from .config_base import ConfigType

class Batch(object):
    """
    will store a collection of filenames that should be downloaded and converted
    from json into parquet.
    """
    def __init__(self, config_type: ConfigType) -> None:
        self.config_type = config_type
        self.filenames = []
        self.total_size = 0

    def __str__(self) -> None:
        return "%d bytes across %d files of type %s" % (self.total_size,
                                                        len(self.filenames),
                                                        self.config_type)

    def add_file(self, filename: str, filesize: int) -> None:
        self.filenames.append(filename)
        self.total_size += filesize

    def trigger_lambda(self) -> None:
        raise Exception("not impled yet")

def batch_files(files: Iterable[(str, int)], threshold: int) -> list[Batch]:
    """
    Take a bunch of files and sort them into Batches based on their config type
    (derrived from filename). Each Batch should be under a limit in total
    filesize.

    :param file: An iterable of filename and filesize tubles to be sorted into
        Batches. The filename is used to determine config type.
    :param threshold: upper bounds on how large the sum filesize of a batch can
        be.

    :return: List of Batches containing all files passed in.
    """
    ongoing_batches = {t: Batch(t) for t in ConfigType}
    complete_batches = []

    # iterate over file tuples, and add them to the ongoing batches. if a batch
    # is going to go past the threshold limit, move it to complete batches, and
    # create a new batch.
    for (filename, size) in files:
        try:
            config_type = ConfigType.from_filename(filename)
        except:
            # for now just print out the error.
            print("Encontered Unknown File Type: %s" % filename)
            continue

        batch = ongoing_batches[config_type]

        if batch.total_size + int(size) > threshold:
            complete_batches.append(batch)
            ongoing_batches[config_type] = Batch(config_type)
            batch = ongoing_batches[config_type]

        batch.add_file(filename, int(size))

    # add the ongoing batches too complete ones and return.
    for (_, batch) in ongoing_batches.items():
        complete_batches.append(batch)

    return complete_batches
