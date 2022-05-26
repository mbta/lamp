import os

from py_gtfs_rt_ingestion import Configuration, gz_to_pyarrow

TEST_FILE_DIR = os.path.join(os.path.dirname(__file__), "test_files")

def test_vehicle_positions_file_conversion(tmpdir):
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    rt_vehicle_positions_file = os.path.join(TEST_FILE_DIR,
        "2022-01-01T00:00:03Z_https_cdn.mbta.com_realtime_VehiclePositions_enhanced.json.gz")
    config = Configuration(filename=rt_vehicle_positions_file)
    table = gz_to_pyarrow(filename=rt_vehicle_positions_file, config=config)
    np_df = table.to_pandas()

    # tuple(na count, dtype, max, min)
    file_details = {
        'year':(0,'int16','2022','2022'),
        'month':(0,'int8','1','1'),
        'day':(0,'int8','1','1'),
        'hour':(0,'int8','0','0'),
        'feed_timestamp':(0,'int64','1640995202','1640995202'),
        'entity_id':(0,'object','y4124','1625'),
        'current_status':(0,'object','STOPPED_AT','INCOMING_AT'),
        'current_stop_sequence':(29,'float64','640.0','1.0'),
        'occupancy_percentage':(426,'float64','nan','nan'),
        'occupancy_status':(190,'object','nan','nan'),
        'stop_id':(29,'object','nan','nan'),
        'vehicle_timestamp':(0,'int64','1640995198','1640994366'),
        'bearing':(0,'int64','360','0'),
        'latitude':(0,'float64','42.778499603271484','41.82632064819336'),
        'longitude':(0,'float64','-70.7895736694336','-71.5481185913086'),
        'speed':(392,'float64','29.9','2.6'),
        'direction_id':(4,'float64','1.0','0.0'),
        'route_id':(0,'object','Shuttle-Generic','1'),
        'schedule_relationship':(0,'object','UNSCHEDULED','ADDED'),
        'start_date':(40,'object','nan','nan'),
        'start_time':(87,'object','nan','nan'),
        'trip_id':(426,'object','nan','nan'),
        'vehicle_id':(0,'object','y4124','1625'),
        'vehicle_label':(0,'object','4124','0420'),
        'vehicle_consist':(324,'object','nan','nan'),
    }

    assert np_df.shape == (426,25)

    all_expected_paths = set(file_details.keys())

    # ensure all of the expected paths were found and there aren't any
    # additional ones
    assert all_expected_paths == set(np_df.columns)

    # check file details
    for col, (na_count, d_type, max, min) in file_details.items():
        print(f"checking: {col}")
        assert na_count == np_df[col].isna().sum()
        assert d_type == np_df[col].dtype
        if max != 'nan':
            assert max == str(np_df[col].max())
        if min != 'nan':
            assert min == str(np_df[col].min())

def test_rt_alert_file_conversion(tmpdir):
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    alerts_file = os.path.join(TEST_FILE_DIR,
        "2022-05-04T15:59:48Z_https_cdn.mbta.com_realtime_Alerts_enhanced.json.gz")

    config = Configuration(filename=alerts_file)
    table = gz_to_pyarrow(filename=alerts_file, config=config)
    np_df = table.to_pandas()

    # tuple(na count, dtype, max, min)
    file_details = {
        'year':(0,'int16','2022','2022'),
        'month':(0,'int8','5','5'),
        'day':(0,'int8','4','4'),
        'hour':(0,'int8','15','15'),
        'feed_timestamp':(0,'int64','1651679986','1651679986'),
        'entity_id':(0,'object','442146','293631'),
        'effect':(0,'object','UNKNOWN_EFFECT','DETOUR'),
        'effect_detail':(0,'object','TRACK_CHANGE','BIKE_ISSUE'),
        'cause':(0,'object','UNKNOWN_CAUSE','CONSTRUCTION'),
        'cause_detail':(0,'object','UNKNOWN_CAUSE','CONSTRUCTION'),
        'severity':(0,'int64','10','0'),
        'severity_level':(0,'object','WARNING','INFO'),
        'created_timestamp':(0,'int64','1651679836','1549051333'),
        'last_modified_timestamp':(0,'int64','1651679848','1549051333'),
        'alert_lifecycle':(0,'object','UPCOMING_ONGOING','NEW'),
        'duration_certainty':(0,'object','UNKNOWN','ESTIMATED'),
        'last_push_notification':(144,'float64','nan','nan'),
        'active_period':(2,'object','nan','nan'),
        'reminder_times':(132,'object','nan','nan'),
        'closed_timestamp':(142,'float64','1651679848.0','1651679682.0'),
        'short_header_text_translation':(0,'object','nan','nan'),
        'header_text_translation':(0,'object','nan','nan'),
        'description_text_translation':(0,'object','nan','nan'),
        'service_effect_text_translation':(0,'object','nan','nan'),
        'timeframe_text_translation':(44,'object','nan','nan'),
        'url_translation':(138,'object','nan','nan'),
        'recurrence_text_translation':(139,'object','nan','nan'),
        'informed_entity':(0,'object','nan','nan'),
    }

    assert np_df.shape == (144,28)

    all_expected_paths = set(file_details.keys())

    # ensure all of the expected paths were found and there aren't any
    # additional ones
    assert all_expected_paths == set(np_df.columns)

    # check file details
    for col, (na_count, d_type, max, min) in file_details.items():
        print(f"checking: {col}")
        assert na_count == np_df[col].isna().sum()
        assert d_type == np_df[col].dtype
        if max != 'nan':
            assert max == str(np_df[col].max())
        if min != 'nan':
            assert min == str(np_df[col].min())



def test_rt_trip_file_conversion(tmpdir):
    """
    TODO - convert a dummy json data to parquet and check that the new file
    matches expectations
    """
    trip_updates_file = os.path.join(TEST_FILE_DIR,
        "2022-05-08T06:04:57Z_https_cdn.mbta.com_realtime_TripUpdates_enhanced.json.gz")

    config = Configuration(filename=trip_updates_file)
    table = gz_to_pyarrow(filename=trip_updates_file, config=config)
    np_df = table.to_pandas()

    # tuple(na count, dtype, max, min)
    file_details = {
        'year':(0,'int16','2022','2022'),
        'month':(0,'int8','5','5'),
        'day':(0,'int8','8','8'),
        'hour':(0,'int8','6','6'),
        'feed_timestamp':(0,'int64','1651989896','1651989896'),
        'entity_id':(0,'object','CR-532710-1518','50922039'),
        'timestamp':(51,'float64','1651989886.0','1651989816.0'),
        'stop_time_update':(0,'object','nan','nan'),
        'direction_id':(0,'int64','1','0'),
        'route_id':(0,'object','SL1','1'),
        'start_date':(50,'object','nan','nan'),
        'start_time':(51,'object','nan','nan'),
        'trip_id':(0,'object','CR-532710-1518','50922039'),
        'route_pattern_id':(79,'object','nan','nan'),
        'schedule_relationship':(79,'object','nan','nan'),
        'vehicle_id':(39,'object','nan','nan'),
        'vehicle_label':(55,'object','nan','nan'),
    }

    assert np_df.shape == (79,17)

    all_expected_paths = set(file_details.keys())

    # ensure all of the expected paths were found and there aren't any
    # additional ones
    assert all_expected_paths == set(np_df.columns)

    # check file details
    for col, (na_count, d_type, max, min) in file_details.items():
        print(f"checking: {col}")
        assert na_count == np_df[col].isna().sum()
        assert d_type == np_df[col].dtype
        if max != 'nan':
            assert max == str(np_df[col].max())
        if min != 'nan':
            assert min == str(np_df[col].min())
