from datetime import datetime, timedelta

from lamp_py.aws.s3 import file_list_from_s3_date_range
from lamp_py.runtime_utils.remote_files import LAMP, S3_SPRINGBOARD
from lamp_py.runtime_utils.remote_files import springboard_rt_vehicle_positions

template = "year={yy}/month={mm}/day={dd}/"
end_date = datetime.now()
end_date = datetime(year=2025, month=3, day=30)
start_date = end_date - timedelta(days=15)  # type: ignore

breakpoint()
s3_uris = file_list_from_s3_date_range(
    bucket_name=S3_SPRINGBOARD,
    file_prefix=springboard_rt_vehicle_positions.prefix,
    path_template=template,
    end_date=end_date,
    start_date=start_date,
)

print(s3_uris)
