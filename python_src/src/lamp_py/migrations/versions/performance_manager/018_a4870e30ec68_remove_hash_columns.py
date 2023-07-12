"""remove hash columns

Revision ID: a4870e30ec68
Revises: 37d97f420d54
Create Date: 2023-07-11 17:40:00.668485

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "a4870e30ec68"
down_revision = "37d97f420d54"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("DROP VIEW IF EXISTS opmi_all_rt_fields_joined;")

    op.create_table(
        "temp_event_compare",
        sa.Column("do_update", sa.Boolean(), nullable=True),
        sa.Column("do_insert", sa.Boolean(), nullable=True),
        sa.Column("pk_id", sa.Integer(), nullable=False),
        sa.Column("new_trip", sa.Boolean(), nullable=True),
        sa.Column("pm_trip_id", sa.Integer(), nullable=True),
        sa.Column("service_date", sa.Integer(), nullable=False),
        sa.Column("direction_id", sa.Boolean(), nullable=False),
        sa.Column("route_id", sa.String(length=60), nullable=False),
        sa.Column("start_time", sa.Integer(), nullable=False),
        sa.Column("vehicle_id", sa.String(length=60), nullable=False),
        sa.Column("stop_sequence", sa.SmallInteger(), nullable=True),
        sa.Column("stop_id", sa.String(length=60), nullable=False),
        sa.Column("parent_station", sa.String(length=60), nullable=False),
        sa.Column("vp_move_timestamp", sa.Integer(), nullable=True),
        sa.Column("vp_stop_timestamp", sa.Integer(), nullable=True),
        sa.Column("tu_stop_timestamp", sa.Integer(), nullable=True),
        sa.Column("trip_id", sa.String(length=128), nullable=True),
        sa.Column("vehicle_label", sa.String(length=128), nullable=True),
        sa.Column("vehicle_consist", sa.String(), nullable=True),
        sa.Column("static_version_key", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("pk_id"),
    )
    op.drop_table("vehicle_event_metrics")
    op.drop_table("temp_hash_compare")
    op.drop_index(
        "ix_static_calendar_dates_timestamp", table_name="static_calendar_dates"
    )
    op.create_index(
        op.f("ix_static_calendar_dates_static_version_key"),
        "static_calendar_dates",
        ["static_version_key"],
        unique=False,
    )
    op.drop_index(
        "ix_static_directions_timestamp", table_name="static_directions"
    )
    op.create_index(
        op.f("ix_static_directions_static_version_key"),
        "static_directions",
        ["static_version_key"],
        unique=False,
    )
    op.drop_index(
        "ix_static_stop_times_stop_id", table_name="static_stop_times"
    )
    op.drop_index(
        "ix_static_stop_times_timestamp", table_name="static_stop_times"
    )
    op.drop_index(
        "ix_static_stop_times_trip_id", table_name="static_stop_times"
    )
    op.create_index(
        "ix_static_stop_times_composite_1",
        "static_stop_times",
        ["static_version_key", "trip_id", "stop_id", "stop_sequence"],
        unique=False,
    )
    op.drop_index("ix_static_stops_stop_id", table_name="static_stops")
    op.drop_index("ix_static_stops_timestamp", table_name="static_stops")
    op.create_index(
        "ix_static_stops_composite_1",
        "static_stops",
        ["static_version_key", "stop_id"],
        unique=False,
    )
    op.drop_index("ix_static_trips_service_id", table_name="static_trips")
    op.drop_index("ix_static_trips_timestamp", table_name="static_trips")
    op.drop_index("ix_static_trips_trip_id", table_name="static_trips")
    op.create_index(
        "ix_static_trips_composite_1",
        "static_trips",
        ["static_version_key", "trip_id", "service_id"],
        unique=False,
    )
    op.add_column(
        "vehicle_events",
        sa.Column("service_date", sa.Integer(), nullable=False),
    )
    op.add_column(
        "vehicle_events", sa.Column("pm_trip_id", sa.Integer(), nullable=False)
    )
    op.add_column(
        "vehicle_events",
        sa.Column("travel_time_seconds", sa.Integer(), nullable=True),
    )
    op.add_column(
        "vehicle_events",
        sa.Column("dwell_time_seconds", sa.Integer(), nullable=True),
    )
    op.add_column(
        "vehicle_events",
        sa.Column("headway_trunk_seconds", sa.Integer(), nullable=True),
    )
    op.add_column(
        "vehicle_events",
        sa.Column("headway_branch_seconds", sa.Integer(), nullable=True),
    )
    op.alter_column(
        "vehicle_events",
        "stop_sequence",
        existing_type=sa.SmallInteger(),
        nullable=True,
    )
    op.drop_index("ix_vehicle_events_trip_hash", table_name="vehicle_events")
    op.drop_index(
        "ix_vehicle_events_trip_stop_hash", table_name="vehicle_events"
    )
    op.create_index(
        "ix_vehicle_events_composite_1",
        "vehicle_events",
        ["service_date", "pm_trip_id", "parent_station"],
        unique=True,
    )
    op.drop_column("vehicle_events", "trip_hash")
    op.drop_column("vehicle_events", "pk_id")
    op.drop_column("vehicle_events", "trip_stop_hash")

    ## TRIPS TABLE ##

    op.create_unique_constraint(
        "vehicle_trips_unique_trip",
        "vehicle_trips",
        [
            "service_date",
            "route_id",
            "direction_id",
            "start_time",
            "vehicle_id",
        ],
    )
    op.drop_column("vehicle_trips", "trip_hash")
    op.add_column(
        "vehicle_trips",
        sa.Column(
            "pm_trip_id",
            sa.Integer(),
            sa.Identity(always=True, start=1, increment=1, cycle=False),
            nullable=False,
        ),
    )
    op.create_primary_key(
        "vehicle_trips_pk",
        "vehicle_trips",
        columns=["service_date", "pm_trip_id"],
    )
    op.create_index(
        "ix_vehicle_trips_composite_1",
        "vehicle_trips",
        ["route_id", "direction_id", "vehicle_id"],
        unique=False,
    )

    update_view = """
        CREATE OR REPLACE VIEW opmi_all_rt_fields_joined AS 
        SELECT
            vt.service_date
            , ve.pm_trip_id as trip_hash
            , NULL as trip_stop_hash
            , ve.stop_sequence
            , ve.stop_id
            , LAG (ve.stop_id, 1) OVER (PARTITION BY ve.pm_trip_id ORDER BY COALESCE(ve.vp_stop_timestamp,  ve.tu_stop_timestamp, ve.vp_move_timestamp)) as previous_stop_id
            , ve.parent_station
            , LAG (ve.parent_station, 1) OVER (PARTITION BY ve.pm_trip_id ORDER BY COALESCE(ve.vp_stop_timestamp,  ve.tu_stop_timestamp, ve.vp_move_timestamp)) as previous_parent_station
            , ve.vp_move_timestamp
            , COALESCE(ve.vp_stop_timestamp,  ve.tu_stop_timestamp) as vp_tu_stop_timestamp
            , vt.direction_id
            , vt.route_id
            , vt.branch_route_id
            , vt.trunk_route_id
            , vt.start_time
            , vt.vehicle_id
            , vt.stop_count
            , vt.trip_id
            , vt.vehicle_label
            , vt.vehicle_consist
            , vt.direction
            , vt.direction_destination
            , vt.static_trip_id_guess
            , vt.static_start_time
            , vt.static_stop_count
            , vt.first_last_station_match
            , vt.static_version_key
            , ve.travel_time_seconds
            , ve.dwell_time_seconds
            , ve.headway_trunk_seconds
            , ve.headway_branch_seconds
            , ve.updated_on
        FROM 
            vehicle_events ve
        LEFT JOIN
            vehicle_trips vt
        ON 
            ve.pm_trip_id = vt.pm_trip_id
        ;
    """
    op.execute(update_view)

    create_rt_braintree_function = """
        CREATE OR REPLACE FUNCTION rt_red_is_braintree_branch(p_trip_id int) RETURNS boolean AS $$ 
        DECLARE
            found_check bool;
        BEGIN 
            SELECT
                true
            INTO
                found_check
            FROM 
                vehicle_events ve
            WHERE 
                ve.pm_trip_id = p_trip_id
                AND ve.stop_id IN ('70097', '70098', '70099', '70100', '70101', '70102', '70103', '70104', '70105', '70095', '70096')
            LIMIT 1
            ;
            IF FOUND THEN
                RETURN true;
            ELSE
                RETURN false;
            END IF;
        END;
        $$ LANGUAGE plpgsql;
    """
    op.execute(create_rt_braintree_function)

    create_rt_ashmont_function = """
        CREATE OR REPLACE FUNCTION rt_red_is_ashmont_branch(p_trip_id int) RETURNS boolean AS $$ 
        DECLARE
            found_check bool;
        BEGIN 
            SELECT
                true
            INTO
                found_check
            FROM 
                vehicle_events ve
            WHERE 
                ve.pm_trip_id = p_trip_id
                AND ve.stop_id IN ('70087', '70088', '70089', '70090', '70091', '70092', '70093', '70094', '70085', '70086')
            LIMIT 1
            ;
            IF FOUND THEN
                RETURN true;
            ELSE
                RETURN false;
            END IF;
        END;
        $$ LANGUAGE plpgsql;
    """
    op.execute(create_rt_ashmont_function)

    create_get_rt_branch_trunk_id = """
        CREATE OR REPLACE FUNCTION update_rt_branch_trunk_id() RETURNS TRIGGER AS $$ 
        BEGIN 
            IF NEW.route_id ~ 'Green-' THEN
                NEW.branch_route_id := NEW.route_id;
                NEW.trunk_route_id := 'Green';
            ELSEIF NEW.route_id = 'Red' THEN
                IF rt_red_is_braintree_branch(NEW.pm_trip_id) THEN
                    NEW.branch_route_id := 'Red-Braintree';
                ELSEIF rt_red_is_ashmont_branch(NEW.pm_trip_id) THEN
                    NEW.branch_route_id := 'Red-Ashmont';
                ELSE
                    NEW.branch_route_id := null;
                END IF;
                NEW.trunk_route_id := NEW.route_id;
            ELSE
                NEW.branch_route_id := null;
                NEW.trunk_route_id := NEW.route_id;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """
    op.execute(create_get_rt_branch_trunk_id)
    drop_trigger = (
        "DROP TRIGGER IF EXISTS rt_trips_update_branch_trunk ON vehicle_trips;"
    )
    op.execute(drop_trigger)
    create_trigger = """
        CREATE TRIGGER rt_trips_update_branch_trunk BEFORE UPDATE ON vehicle_trips 
        FOR EACH ROW EXECUTE PROCEDURE update_rt_branch_trunk_id();
    """
    op.execute(create_trigger)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS opmi_all_rt_fields_joined;")
    op.execute("TRUNCATE vehicle_trips, vehicle_events;")

    op.add_column(
        "vehicle_trips",
        sa.Column(
            "trip_hash", postgresql.BYTEA(), autoincrement=False, nullable=False
        ),
    )
    op.drop_constraint(
        "vehicle_trips_unique_trip", "vehicle_trips", type_="unique"
    )
    op.drop_index("ix_vehicle_trips_composite_1", table_name="vehicle_trips")
    op.drop_constraint("vehicle_trips_pk", "vehicle_trips", type_="primary")
    op.drop_column("vehicle_trips", "pm_trip_id")
    op.add_column(
        "vehicle_events",
        sa.Column(
            "trip_stop_hash",
            postgresql.BYTEA(),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.add_column(
        "vehicle_events",
        sa.Column("pk_id", sa.INTEGER(), autoincrement=True, nullable=False),
    )
    op.add_column(
        "vehicle_events",
        sa.Column(
            "trip_hash", postgresql.BYTEA(), autoincrement=False, nullable=False
        ),
    )
    op.drop_index("ix_vehicle_events_composite_1", table_name="vehicle_events")
    op.create_index(
        "ix_vehicle_events_trip_stop_hash",
        "vehicle_events",
        ["trip_stop_hash"],
        unique=False,
    )
    op.create_index(
        "ix_vehicle_events_trip_hash",
        "vehicle_events",
        ["trip_hash"],
        unique=False,
    )
    op.drop_column("vehicle_events", "headway_branch_seconds")
    op.drop_column("vehicle_events", "headway_trunk_seconds")
    op.drop_column("vehicle_events", "dwell_time_seconds")
    op.drop_column("vehicle_events", "travel_time_seconds")
    op.drop_column("vehicle_events", "pm_trip_id")
    op.drop_column("vehicle_events", "service_date")
    op.drop_index("ix_static_trips_composite_1", table_name="static_trips")
    op.create_index(
        "ix_static_trips_trip_id", "static_trips", ["trip_id"], unique=False
    )
    op.create_index(
        "ix_static_trips_timestamp",
        "static_trips",
        ["static_version_key"],
        unique=False,
    )
    op.create_index(
        "ix_static_trips_service_id",
        "static_trips",
        ["service_id"],
        unique=False,
    )
    op.drop_index("ix_static_stops_composite_1", table_name="static_stops")
    op.create_index(
        "ix_static_stops_timestamp",
        "static_stops",
        ["static_version_key"],
        unique=False,
    )
    op.create_index(
        "ix_static_stops_stop_id", "static_stops", ["stop_id"], unique=False
    )
    op.drop_index(
        "ix_static_stop_times_composite_1", table_name="static_stop_times"
    )
    op.create_index(
        "ix_static_stop_times_trip_id",
        "static_stop_times",
        ["trip_id"],
        unique=False,
    )
    op.create_index(
        "ix_static_stop_times_timestamp",
        "static_stop_times",
        ["static_version_key"],
        unique=False,
    )
    op.create_index(
        "ix_static_stop_times_stop_id",
        "static_stop_times",
        ["stop_id"],
        unique=False,
    )
    op.drop_index(
        op.f("ix_static_directions_static_version_key"),
        table_name="static_directions",
    )
    op.create_index(
        "ix_static_directions_timestamp",
        "static_directions",
        ["static_version_key"],
        unique=False,
    )
    op.drop_index(
        op.f("ix_static_calendar_dates_static_version_key"),
        table_name="static_calendar_dates",
    )
    op.create_index(
        "ix_static_calendar_dates_timestamp",
        "static_calendar_dates",
        ["static_version_key"],
        unique=False,
    )
    op.create_table(
        "temp_hash_compare",
        sa.Column(
            "hash", postgresql.BYTEA(), autoincrement=False, nullable=False
        ),
        sa.PrimaryKeyConstraint("hash", name="temp_hash_compare_pkey"),
    )
    op.create_table(
        "vehicle_event_metrics",
        sa.Column(
            "trip_stop_hash",
            postgresql.BYTEA(),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "travel_time_seconds",
            sa.INTEGER(),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "dwell_time_seconds",
            sa.INTEGER(),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "headway_trunk_seconds",
            sa.INTEGER(),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "headway_branch_seconds",
            sa.INTEGER(),
            autoincrement=False,
            nullable=True,
        ),
        sa.Column(
            "updated_on",
            postgresql.TIMESTAMP(),
            server_default=sa.text("now()"),
            autoincrement=False,
            nullable=True,
        ),
        sa.PrimaryKeyConstraint(
            "trip_stop_hash", name="vehicle_event_metrics_pkey"
        ),
    )
    op.drop_table("temp_event_compare")

    update_view = """
        CREATE OR REPLACE VIEW opmi_all_rt_fields_joined AS 
        SELECT
            vt.service_date
            , ve.trip_hash
            , ve.trip_stop_hash
            , ve.stop_sequence
            , ve.stop_id
            , LAG (ve.stop_id, 1) OVER (PARTITION BY ve.trip_hash ORDER BY COALESCE(ve.vp_stop_timestamp,  ve.tu_stop_timestamp, ve.vp_move_timestamp)) as previous_stop_id
            , ve.parent_station
            , LAG (ve.parent_station, 1) OVER (PARTITION BY ve.trip_hash ORDER BY COALESCE(ve.vp_stop_timestamp,  ve.tu_stop_timestamp, ve.vp_move_timestamp)) as previous_parent_station
            , ve.vp_move_timestamp
            , COALESCE(ve.vp_stop_timestamp,  ve.tu_stop_timestamp) as vp_tu_stop_timestamp
            , vt.direction_id
            , vt.route_id
            , vt.branch_route_id
            , vt.trunk_route_id
            , vt.start_time
            , vt.vehicle_id
            , vt.stop_count
            , vt.trip_id
            , vt.vehicle_label
            , vt.vehicle_consist
            , vt.direction
            , vt.direction_destination
            , vt.static_trip_id_guess
            , vt.static_start_time
            , vt.static_stop_count
            , vt.first_last_station_match
            , vt.static_version_key
            , vem.travel_time_seconds
            , vem.dwell_time_seconds
            , vem.headway_trunk_seconds
            , vem.headway_branch_seconds
            , COALESCE(vem.updated_on, ve.updated_on) as updated_on
        FROM 
            vehicle_events ve
        LEFT JOIN
            vehicle_trips vt
        ON 
            ve.trip_hash = vt.trip_hash
        LEFT JOIN 
            vehicle_event_metrics vem
        ON
            ve.trip_stop_hash = vem.trip_stop_hash
        ;
    """

    op.execute(update_view)

    create_rt_braintree_function = """
        CREATE OR REPLACE FUNCTION rt_red_is_braintree_branch(p_trip_hash bytea) RETURNS boolean AS $$ 
        DECLARE
            found_check bool;
        BEGIN 
            SELECT
                true
            INTO
                found_check
            FROM 
                vehicle_events ve
            WHERE 
                ve.trip_hash = p_trip_hash
                AND ve.stop_id IN ('70097', '70098', '70099', '70100', '70101', '70102', '70103', '70104', '70105', '70095', '70096')
            LIMIT 1
            ;
            IF FOUND THEN
                RETURN true;
            ELSE
                RETURN false;
            END IF;
        END;
        $$ LANGUAGE plpgsql;
    """
    op.execute(create_rt_braintree_function)

    create_rt_ashmont_function = """
        CREATE OR REPLACE FUNCTION rt_red_is_ashmont_branch(p_trip_hash bytea) RETURNS boolean AS $$ 
        DECLARE
            found_check bool;
        BEGIN 
            SELECT
                true
            INTO
                found_check
            FROM 
                vehicle_events ve
            WHERE 
                ve.trip_hash = p_trip_hash
                AND ve.stop_id IN ('70087', '70088', '70089', '70090', '70091', '70092', '70093', '70094', '70085', '70086')
            LIMIT 1
            ;
            IF FOUND THEN
                RETURN true;
            ELSE
                RETURN false;
            END IF;
        END;
        $$ LANGUAGE plpgsql;
    """
    op.execute(create_rt_ashmont_function)

    create_get_rt_branch_trunk_id = """
        CREATE OR REPLACE FUNCTION update_rt_branch_trunk_id() RETURNS TRIGGER AS $$ 
        BEGIN 
            IF NEW.route_id ~ 'Green-' THEN
                NEW.branch_route_id := NEW.route_id;
                NEW.trunk_route_id := 'Green';
            ELSEIF NEW.route_id = 'Red' THEN
                IF rt_red_is_braintree_branch(NEW.trip_hash) THEN
                    NEW.branch_route_id := 'Red-Braintree';
                ELSEIF rt_red_is_ashmont_branch(NEW.trip_hash) THEN
                    NEW.branch_route_id := 'Red-Ashmont';
                ELSE
                    NEW.branch_route_id := null;
                END IF;
                NEW.trunk_route_id := NEW.route_id;
            ELSE
                NEW.branch_route_id := null;
                NEW.trunk_route_id := NEW.route_id;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """
    op.execute(create_get_rt_branch_trunk_id)
    drop_trigger = (
        "DROP TRIGGER IF EXISTS rt_trips_update_branch_trunk ON vehicle_trips;"
    )
    op.execute(drop_trigger)
    create_trigger = """
        CREATE TRIGGER rt_trips_update_branch_trunk BEFORE INSERT OR UPDATE ON vehicle_trips 
        FOR EACH ROW EXECUTE PROCEDURE update_rt_branch_trunk_id();
    """
    op.execute(create_trigger)
