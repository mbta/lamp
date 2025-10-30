from datetime import date
import dataframely as dy
import polars as pl
from lamp_py.bus_performance_manager.events_tm import TMDailyWorkPiece, create_public_operator_id_map


def test_create_public_operator_id_map() -> None:
    """
    Basic test to show functionality of public_operator_id creation. 
    """
    # ┌────────────┬───────────────────────┬──────────────┬────────────────────┐
    # │ tm_trip_id ┆ operator_badge_number ┆ service_date ┆ public_operator_id │
    # │ ---        ┆ ---                   ┆ ---          ┆ ---                │
    # │ str        ┆ str                   ┆ date         ┆ i64                │
    # ╞════════════╪═══════════════════════╪══════════════╪════════════════════╡
    # │ 123        ┆ oy@\a_BRaC'  ;r>      ┆ 2025-10-31   ┆ 2025103160494      │
    # │ 456        ┆ IDejv                 ┆ 2025-10-31   ┆ 2025103165125      │
    # │ 789        ┆ j9                    ┆ 2025-10-31   ┆ 2025103115306      │
    # └────────────┴───────────────────────┴──────────────┴────────────────────┘

    test_df = TMDailyWorkPiece.sample(
        num_rows=3,
        overrides={
            "tm_trip_id": ["123", "456", "789"],
            # "public_operator_id": [None, None, None, None, None],
            "service_date": [
                date(year=2025, month=10, day=31),
                date(year=2025, month=10, day=31),
                date(year=2025, month=10, day=31),
                # date(year=2025, month=10, day=30),
                # date(year=2025, month=10, day=30),
            ],
        },
        generator=dy.random.Generator(seed=0),
    )
    output_df = create_public_operator_id_map(test_df, seed=0)

    assert output_df["public_operator_id"].n_unique() == 3
    assert output_df["public_operator_id"].equals(pl.Series([2025103160494, 2025103165125, 2025103115306]))


# rule - operator id and trip_id have same # of unique
# rule - operator_id for the same trip are all the same
