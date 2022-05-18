from .config_base import ConfigType

class Batch(object):
    """
    will store a collection of filenames that should be downloaded and converted
    from json into parquet.
    """
    def __init__(self, config_type) -> None:
        self.config_type = config_type
        self.filenames = []
        self.total_size = 0

    def __str__(self) -> None:
        return "%d mb across %d files of type %s" % (self.total_size,
                                                     len(self.filenames),
                                                     self.config_type)

    def add_file(self, filename, filesize) -> None:
        self.filenames.append(filename)
        self.total_size += filesize

    def trigger_lambda(self) -> None:
        raise Exception("not impled yet")

def batch_files(list_of_files, threshold) -> list:
    complete_batches = []
    ongoing_batches = {
        ConfigType.RT_ALERTS: Batch(ConfigType.RT_ALERTS),
        ConfigType.RT_TRIP_UPDATES: Batch(ConfigType.RT_TRIP_UPDATES),
        ConfigType.RT_VEHICLE_POSITIONS: Batch(ConfigType.RT_VEHICLE_POSITIONS)
    }

    for file_info in list_of_files:
        (date, time, size, filename) = file_info.split()

        try:
            config_type = ConfigType.from_filename(filename)
            batch = ongoing_batches[config_type]
        except:
            # unable to figure out config type, continue _for now_
            continue

        if batch.total_size + int(size) > threshold:
            complete_batches.append(batch)
            ongoing_batches[config_type] = Batch(config_type)
            batch = ongoing_batches[config_type]

        batch.add_file(filename, int(size))

    for (_, batch) in ongoing_batches.items():
        complete_batches.append(batch)

    return complete_batches


