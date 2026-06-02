
from analysis.bus_prediction_analyzer_utils import AnalyzerConfig, add_error_columns, assign_ibi_bin, assign_time_of_day_bin, calculate_accuracy_by_group, calculate_ibi_accuracy, default_config, detect_prediction_bias, is_prediction_accurate, join_tu_vp, narrow_trip_updates, narrow_vehicle_positions, parse_start_time_seconds

import polars as pl

def run_analysis(
    tu_df: pl.DataFrame,
    vp_df: pl.DataFrame,
    config: AnalyzerConfig = default_config(),
) -> dict[str, pl.DataFrame]:
    """Orchestrate the full bus prediction analysis pipeline.

    Runs all processing steps in sequence:
    1. Narrow VP and TU to canonical columns
    2. Join TU and VP
    3. Add error columns
    4. Assign IBI bins
    5. Flag accurate predictions
    6. Calculate IBI accuracy (overall)
    7. Assign time-of-day bins
    8. Parse start_time seconds
    9. Calculate accuracy by group (route, time_of_day, ibi_bin)
    10. Detect prediction bias per trip

    Args:
        tu_df: Raw trip_updates DataFrame
        vp_df: Raw vehicle_positions DataFrame
        config: AnalyzerConfig with bins and thresholds

    Returns:
        Dict with keys:
        - 'joined': Full joined dataframe with all computed columns
        - 'ibi_accuracy': Overall IBI accuracy by bin
        - 'route_accuracy': Accuracy grouped by route_id
        - 'time_of_day_accuracy': Accuracy grouped by time_of_day_bin
        - 'bias': Prediction bias per trip
    """
    # Step 1-2: Narrow and join
    tu_narrow = narrow_trip_updates(tu_df)
    vp_narrow = narrow_vehicle_positions(vp_df)
    joined = join_tu_vp(tu_narrow, vp_narrow)

    # Step 3: Add error columns
    with_errors = add_error_columns(joined)

    # Step 4-5: IBI binning and accuracy
    with_ibi_bin = assign_ibi_bin(with_errors, config)
    with_accuracy = is_prediction_accurate(with_ibi_bin, config)

    # Step 6: Overall IBI accuracy
    ibi_accuracy = calculate_ibi_accuracy(with_accuracy, config)

    # Step 7-8: Time-of-day binning and start_time parsing
    with_tod_bin = assign_time_of_day_bin(with_accuracy, config)
    with_start_time = parse_start_time_seconds(with_tod_bin)

    # Step 9: Grouped accuracy (by route, by time_of_day, by ibi_bin)
    route_accuracy = calculate_accuracy_by_group(with_start_time, ["route_id"], config)
    time_of_day_accuracy = calculate_accuracy_by_group(
        with_start_time, ["time_of_day_bin"], config
    )

    # Step 10: Prediction bias
    bias = detect_prediction_bias(with_start_time)

    return {
        "joined": with_start_time,
        "ibi_accuracy": ibi_accuracy,
        "route_accuracy": route_accuracy,
        "time_of_day_accuracy": time_of_day_accuracy,
        "bias": bias,
    }

