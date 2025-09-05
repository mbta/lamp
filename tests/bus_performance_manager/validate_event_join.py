import pointblank as pb
import polars as pl
from datetime import date

from lamp_py.bus_performance_manager.combined_bus_schedule import join_tm_schedule_to_gtfs_schedule
from lamp_py.bus_performance_manager.events_gtfs_rt import generate_gtfs_rt_events
from lamp_py.bus_performance_manager.events_gtfs_schedule import bus_gtfs_schedule_events_for_date
from lamp_py.bus_performance_manager.events_tm import generate_tm_events
from lamp_py.bus_performance_manager.events_joined import join_rt_to_schedule
from lamp_py.bus_performance_manager.events_tm_schedule import generate_tm_schedule
from lamp_py.bus_performance_manager.events_metrics import enrich_bus_performance_metrics
from lamp_py.bus_performance_manager.event_files import event_files_to_load

service_date = date(2025, 8, 14)

try:
    combined_schedule = pl.read_parquet(f"/tmp/combined_schedule_{str(service_date)}.parquet")
    tm_events = pl.read_parquet(f"/tmp/tm_events_{str(service_date)}.parquet")
    gtfs_events = pl.read_parquet(f"/tmp/gtfs_events_{str(service_date)}.parquet")

except FileNotFoundError:
    tm_schedule = generate_tm_schedule()
    combined_schedule = join_tm_schedule_to_gtfs_schedule(bus_gtfs_schedule_events_for_date(service_date), tm_schedule)

    event_files = event_files_to_load(service_date, service_date)
    tm_events = generate_tm_events(event_files[service_date]["transit_master"], tm_schedule)
    gtfs_events = generate_gtfs_rt_events(service_date, event_files[service_date]["gtfs_rt"])

    combined_schedule.write_parquet(f"/tmp/combined_schedule_{str(service_date)}.parquet")
    tm_events.write_parquet(f"/tmp/tm_events_{str(service_date)}.parquet")
    gtfs_events.write_parquet(f"/tmp/gtfs_events_{str(service_date)}.parquet")

bus_events = enrich_bus_performance_metrics(join_rt_to_schedule(combined_schedule, gtfs_events, tm_events))

validation = (
    pb.Validate(bus_events, thresholds = pb.Thresholds(error = 1))
    .col_vals_regex("trip_id", r"[0-9]+", brief = "trip_id consists of only digits")
    .interrogate()
)

validation
