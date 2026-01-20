# just run the bus pipeline
from datetime import date
from lamp_py.bus_performance_manager.events_metrics import run_bus_performance_pipeline
from lamp_py.bus_performance_manager.event_files import event_files_to_load

service_date = date(2025, 11, 18)
event_files = event_files_to_load(service_date, service_date)

for service_date in event_files.keys():
    gtfs_files = event_files[service_date]["gtfs_rt"]
    tm_files = event_files[service_date]["transit_master_stop_crossing"]
    tm_files_work_pieces = event_files[service_date]["transit_master_daily_work_piece"]

    events_df, operator_id_mapping = run_bus_performance_pipeline(
        service_date, gtfs_files, tm_files, tm_files_work_pieces
    )
