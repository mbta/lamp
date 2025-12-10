from datetime import date, timedelta
import datetime


# Create a DataFrame with two date columns
def build_data_range_paths(template_string: str, start_date: date, end_date: date) -> list[str]:
    """
    Given an f-string template, fill in the {} in template with all the days between
    start_date and end_date (inclusive) and return the result as a list of strings
    """

    # add 1 for inclusive
    date_diff_days = (start_date - end_date).days * -1 + 1

    date_paths = []
    #
    for i in range(0, date_diff_days):
        tmp = start_date + timedelta(days=i)

        # wrong format - good for delta though
        # prefix_date_part = f"{yy}/{mm:02d}/{dd:02d}"

        # prefix_date_part = f"year={yy}/month={mm}/day={dd}/"
        # prefix_whole_path = f"year={yy}/month={mm}/day={dd}/{yy}-{mm:02d}-{dd:02d}T00:00:00.parquet"

        formatted = template_string.format(yy=tmp.year, mm=tmp.month, dd=tmp.day)
        date_paths.append(formatted)
    return date_paths


def build_time_range_paths(
    template_string: str,
    start_time: datetime.time,
    end_time: datetime.time,
    delta: datetime.timedelta = datetime.timedelta(seconds=1),
) -> list[str]:
    """
    Given an f-string template, fill in the {} in template with all the days between
    start_date and end_date (inclusive) and return the result as a list of strings
    """

    # loop up by seconds
    # add 1 for inclusive
    date_diff_days = (start_time - end_time).days * -1 + 1

    date_paths = []
    #
    for i in range(0, date_diff_days):
        tmp = start_time + timedelta(days=i)

        formatted = template_string.format(hh=tmp.hh, min=tmp.min, dd=tmp.day)
        date_paths.append(formatted)
    return date_paths
