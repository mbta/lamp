"""timestamp to static_version_key

Revision ID: 4fe83fd4091d
Revises: 18d962b3df59
Create Date: 2023-05-19 09:52:16.177008

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "4fe83fd4091d"
down_revision = "18d962b3df59"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "vehicle_events_fk_static_timestamp_fkey",
        "vehicle_events",
        type_="foreignkey",
    )
    op.drop_constraint(
        "vehicle_trips_fk_static_timestamp_fkey",
        "vehicle_trips",
        type_="foreignkey",
    )
    op.drop_constraint(
        "static_feed_info_timestamp_key", "static_feed_info", type_="unique"
    )

    op.drop_column("vehicle_events", "fk_static_timestamp")

    op.alter_column(
        "static_feed_info", "timestamp", new_column_name="static_version_key"
    )
    op.create_unique_constraint(
        "static_feed_info_static_version_key",
        "static_feed_info",
        ["static_version_key"],
    )

    op.alter_column(
        "vehicle_trips",
        "fk_static_timestamp",
        new_column_name="static_version_key",
    )
    op.create_foreign_key(
        "vehicle_trips_static_version_key_fkey",
        "vehicle_trips",
        "static_feed_info",
        ["static_version_key"],
        ["static_version_key"],
    )

    op.alter_column(
        "static_calendar", "timestamp", new_column_name="static_version_key"
    )
    op.create_index(
        op.f("ix_static_calendar_static_version_key"),
        "static_calendar",
        ["static_version_key"],
        unique=False,
    )
    op.alter_column(
        "static_calendar_dates",
        "timestamp",
        new_column_name="static_version_key",
    )
    op.alter_column(
        "static_directions", "timestamp", new_column_name="static_version_key"
    )
    op.alter_column(
        "static_routes", "timestamp", new_column_name="static_version_key"
    )
    op.create_index(
        op.f("ix_static_routes_static_version_key"),
        "static_routes",
        ["static_version_key"],
        unique=False,
    )
    op.alter_column(
        "static_stop_times", "timestamp", new_column_name="static_version_key"
    )
    op.alter_column(
        "static_stops", "timestamp", new_column_name="static_version_key"
    )
    op.alter_column(
        "static_trips", "timestamp", new_column_name="static_version_key"
    )

    upgrade_static_ashmont_func = """
        CREATE OR REPLACE FUNCTION red_is_ashmont_branch(p_trip_id varchar, p_fk_ts int) RETURNS boolean AS $$ 
        DECLARE
            is_ashmont boolean;
        BEGIN 
            SELECT
                true
            INTO
                is_ashmont
            FROM 
                static_stop_times sst
            WHERE 
                sst.trip_id = p_trip_id
                AND sst.static_version_key = p_fk_ts
                AND sst.stop_id IN ('70087', '70088', '70089', '70090', '70091', '70092', '70093', '70094', '70085', '70086')
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
    op.execute(upgrade_static_ashmont_func)

    upgrade_static_braintree_func = """
        CREATE OR REPLACE FUNCTION red_is_braintree_branch(p_trip_id varchar, p_fk_ts int) RETURNS boolean AS $$ 
        DECLARE	
            is_braintree boolean;
        BEGIN 
            SELECT
                true
            INTO
                is_braintree
            FROM 
                static_stop_times sst
            WHERE 
                sst.trip_id = p_trip_id
                AND sst.static_version_key = p_fk_ts
                AND sst.stop_id IN ('70097', '70098', '70099', '70100', '70101', '70102', '70103', '70104', '70105', '70095', '70096')
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
    op.execute(upgrade_static_braintree_func)

    upgrade_branch_trunk_function = """
        CREATE OR REPLACE FUNCTION insert_static_trips_branch_trunk() RETURNS TRIGGER AS $$ 
        BEGIN 
            IF NEW.route_id ~ 'Green-' THEN
                NEW.branch_route_id := NEW.route_id;
                NEW.trunk_route_id := 'Green';
            ELSEIF NEW.route_id = 'Red' THEN
                IF red_is_braintree_branch(NEW.trip_id, NEW.static_version_key) THEN
                    NEW.branch_route_id := 'Red-Braintree';
                ELSEIF red_is_ashmont_branch(NEW.trip_id, NEW.static_version_key) THEN
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
    op.execute(upgrade_branch_trunk_function)

    alter_view = 'ALTER VIEW static_service_id_lookup RENAME COLUMN "timestamp" TO static_version_key;'
    op.execute(alter_view)
    upgrade_view = """
        CREATE OR REPLACE VIEW static_service_id_lookup AS 
        WITH mod_feed_dates AS (
            SELECT 
                feed_start_date::text::date AS start_date,
                feed_end_date::text::date AS end_date,
                static_version_key
            FROM public.static_feed_info
        ), sd_match AS (
            SELECT 
                replace(mod_sd::date::text, '-', '')::integer AS service_date,
                mod_feed_dates.static_version_key
            FROM mod_feed_dates,
                generate_series(mod_feed_dates.start_date, mod_feed_dates.end_date, '1 day') AS mod_sd
        ), static_cal AS (
            SELECT
                service_id, 
                start_date::text::date AS start_date, 
                end_date::text::date AS end_date,
                static_version_key,
                monday,
                tuesday,
                wednesday,
                thursday,
                friday,
                saturday,
                sunday
            FROM 
                public.static_calendar
            WHERE 
                public.static_calendar.static_version_key >= (SELECT min(static_version_key) FROM public.static_calendar_dates)
        ), service_ids AS (
            SELECT
                static_cal.service_id,
                replace(new_sd::date::text, '-', '')::integer AS service_date,
                static_cal.static_version_key
            FROM static_cal,
                generate_series(static_cal.start_date, static_cal.end_date, '1 day') AS new_sd
            WHERE 
                (extract(dow from new_sd) = 0 AND static_cal.sunday)
                OR (extract(dow from new_sd) = 1 AND static_cal.monday)
                OR (extract(dow from new_sd) = 2 AND static_cal.tuesday)
                OR (extract(dow from new_sd) = 3 AND static_cal.wednesday)
                OR (extract(dow from new_sd) = 4 AND static_cal.thursday)
                OR (extract(dow from new_sd) = 5 AND static_cal.friday)
                OR (extract(dow from new_sd) = 6 AND static_cal.saturday)
        ), service_ids_special AS (
            SELECT 
                scd.service_id,
                scd."date" AS service_date,
                scd.static_version_key
            FROM 
                public.static_calendar_dates scd
            WHERE 
                exception_type = 1
        ), service_ids_exclude AS (
            SELECT 
                scd.service_id,
                scd."date" AS service_date,
                scd.static_version_key,
                true AS to_exclude
            FROM 
                public.static_calendar_dates scd
            WHERE 
                exception_type = 2
        ), all_service_ids AS (
            SELECT 
                service_id,
                service_date,
                static_version_key
            FROM 
                service_ids
            UNION
            SELECT
                service_id,
                service_date,
                static_version_key
            FROM 
                service_ids_special
        )
        SELECT
            st.route_id,
            a_sid.service_id,
            a_sid.service_date,
            a_sid.static_version_key
        FROM 
            all_service_ids a_sid
        FULL OUTER JOIN
            service_ids_exclude e_sid
        ON 
            a_sid.service_id = e_sid.service_id
            AND a_sid.service_date = e_sid.service_date
            AND a_sid.static_version_key = e_sid.static_version_key
        JOIN sd_match
        ON
            sd_match.static_version_key = a_sid.static_version_key
            AND sd_match.service_date = a_sid.service_date
        JOIN 
            public.static_trips st 
        ON
            a_sid.service_id = st.service_id 
            AND a_sid.static_version_key = st.static_version_key
        WHERE
            e_sid.to_exclude IS null
        GROUP BY
            st.route_id,
            a_sid.service_id,
            a_sid.service_date,
            a_sid.static_version_key
        ;
    """
    op.execute(upgrade_view)

    alter_view = 'ALTER VIEW service_id_by_date_and_route RENAME COLUMN "timestamp" TO static_version_key;'
    op.execute(alter_view)
    upgrade_view = """
        CREATE OR REPLACE VIEW service_id_by_date_and_route AS 
        WITH mod_feed_dates AS (
            SELECT 
                feed_start_date::text::date AS start_date,
                lead(feed_start_date,1,replace(CURRENT_DATE::text, '-', '')::integer) OVER (ORDER BY feed_start_date)::text::date - 1 AS end_date,
                static_version_key
            FROM public.static_feed_info
        ), sd_match AS (
            SELECT 
                replace(mod_sd::date::text, '-', '')::integer AS service_date,
                mod_feed_dates.static_version_key
            FROM mod_feed_dates,
                generate_series(mod_feed_dates.start_date, mod_feed_dates.end_date, '1 day') AS mod_sd
        ), static_cal AS (
            SELECT
                service_id, 
                start_date::text::date AS start_date, 
                end_date::text::date AS end_date,
                static_version_key,
                monday,
                tuesday,
                wednesday,
                thursday,
                friday,
                saturday,
                sunday
            FROM 
                public.static_calendar
            WHERE 
                public.static_calendar.static_version_key >= (SELECT min(static_version_key) FROM public.static_calendar_dates)
        ), service_ids AS (
            SELECT
                static_cal.service_id,
                replace(new_sd::date::text, '-', '')::integer AS service_date,
                static_cal.static_version_key
            FROM static_cal,
                generate_series(static_cal.start_date, static_cal.end_date, '1 day') AS new_sd
            WHERE 
                (extract(dow from new_sd) = 0 AND static_cal.sunday)
                OR (extract(dow from new_sd) = 1 AND static_cal.monday)
                OR (extract(dow from new_sd) = 2 AND static_cal.tuesday)
                OR (extract(dow from new_sd) = 3 AND static_cal.wednesday)
                OR (extract(dow from new_sd) = 4 AND static_cal.thursday)
                OR (extract(dow from new_sd) = 5 AND static_cal.friday)
                OR (extract(dow from new_sd) = 6 AND static_cal.saturday)
        ), service_ids_special AS (
            SELECT 
                scd.service_id,
                scd."date" AS service_date,
                scd.static_version_key
            FROM 
                public.static_calendar_dates scd
            WHERE 
                exception_type = 1
        ), service_ids_exclude AS (
            SELECT 
                scd.service_id,
                scd."date" AS service_date,
                scd.static_version_key,
                true AS to_exclude
            FROM 
                public.static_calendar_dates scd
            WHERE 
                exception_type = 2
        ), all_service_ids AS (
            SELECT 
                service_id,
                service_date,
                static_version_key
            FROM 
                service_ids
            UNION
            SELECT
                service_id,
                service_date,
                static_version_key
            FROM 
                service_ids_special
        )
        SELECT
            st.route_id,
            a_sid.service_id,
            a_sid.service_date,
            a_sid.static_version_key
        FROM 
            all_service_ids a_sid
        FULL OUTER JOIN
            service_ids_exclude e_sid
        ON 
            a_sid.service_id = e_sid.service_id
            AND a_sid.service_date = e_sid.service_date
            AND a_sid.static_version_key = e_sid.static_version_key
        JOIN sd_match
        ON
            sd_match.static_version_key = a_sid.static_version_key
            AND sd_match.service_date = a_sid.service_date
        JOIN 
            public.static_trips st 
        ON
            a_sid.service_id = st.service_id 
            AND a_sid.static_version_key = st.static_version_key
        WHERE
            e_sid.to_exclude IS null
            AND st.route_id IN ('Red', 'Mattapan', 'Orange', 'Green-B', 'Green-C', 'Green-D', 'Green-E', 'Blue')
        GROUP BY
            st.route_id,
            a_sid.service_id,
            a_sid.service_date,
            a_sid.static_version_key
        ;
    """
    op.execute(upgrade_view)


def downgrade() -> None:
    op.drop_index(
        op.f("ix_static_calendar_static_version_key"),
        table_name="static_calendar",
    )
    op.alter_column(
        "static_calendar", "static_version_key", new_column_name="timestamp"
    )
    op.alter_column(
        "static_calendar_dates",
        "static_version_key",
        new_column_name="timestamp",
    )
    op.alter_column(
        "static_directions", "static_version_key", new_column_name="timestamp"
    )
    op.drop_index(
        op.f("ix_static_routes_static_version_key"), table_name="static_routes"
    )
    op.alter_column(
        "static_routes", "static_version_key", new_column_name="timestamp"
    )
    op.alter_column(
        "static_stop_times", "static_version_key", new_column_name="timestamp"
    )
    op.alter_column(
        "static_stops", "static_version_key", new_column_name="timestamp"
    )
    op.alter_column(
        "static_trips", "static_version_key", new_column_name="timestamp"
    )

    op.drop_constraint(
        "vehicle_trips_static_version_key_fkey",
        "vehicle_trips",
        type_="foreignkey",
    )
    op.drop_constraint(
        "static_feed_info_static_version_key",
        "static_feed_info",
        type_="unique",
    )

    op.alter_column(
        "static_feed_info", "static_version_key", new_column_name="timestamp"
    )
    op.create_unique_constraint(
        "static_feed_info_timestamp_key", "static_feed_info", ["timestamp"]
    )

    op.alter_column(
        "vehicle_trips",
        "static_version_key",
        new_column_name="fk_static_timestamp",
    )
    op.create_foreign_key(
        "vehicle_trips_fk_static_timestamp_fkey",
        "vehicle_trips",
        "static_feed_info",
        ["fk_static_timestamp"],
        ["timestamp"],
    )

    op.add_column(
        "vehicle_events",
        sa.Column(
            "fk_static_timestamp",
            sa.INTEGER(),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.create_foreign_key(
        "vehicle_events_fk_static_timestamp_fkey",
        "vehicle_events",
        "static_feed_info",
        ["fk_static_timestamp"],
        ["timestamp"],
    )

    downgrade_static_ashmont_func = """
        CREATE OR REPLACE FUNCTION red_is_ashmont_branch(p_trip_id varchar, p_fk_ts int) RETURNS boolean AS $$ 
        DECLARE
            is_ashmont boolean;
        BEGIN 
            SELECT
                true
            INTO
                is_ashmont
            FROM 
                static_stop_times sst
            WHERE 
                sst.trip_id = p_trip_id
                AND sst."timestamp" = p_fk_ts
                AND sst.stop_id IN ('70087', '70088', '70089', '70090', '70091', '70092', '70093', '70094', '70085', '70086')
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
    op.execute(downgrade_static_ashmont_func)

    downgrade_static_braintree_func = """
        CREATE OR REPLACE FUNCTION red_is_braintree_branch(p_trip_id varchar, p_fk_ts int) RETURNS boolean AS $$ 
        DECLARE	
            is_braintree boolean;
        BEGIN 
            SELECT
                true
            INTO
                is_braintree
            FROM 
                static_stop_times sst
            WHERE 
                sst.trip_id = p_trip_id
                AND sst."timestamp" = p_fk_ts
                AND sst.stop_id IN ('70097', '70098', '70099', '70100', '70101', '70102', '70103', '70104', '70105', '70095', '70096')
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
    op.execute(downgrade_static_braintree_func)

    downgrade_branch_trunk_function = """
        CREATE OR REPLACE FUNCTION insert_static_trips_branch_trunk() RETURNS TRIGGER AS $$ 
        BEGIN 
            IF NEW.route_id ~ 'Green-' THEN
                NEW.branch_route_id := NEW.route_id;
                NEW.trunk_route_id := 'Green';
            ELSEIF NEW.route_id = 'Red' THEN
                IF red_is_braintree_branch(NEW.trip_id, NEW."timestamp") THEN
                    NEW.branch_route_id := 'Red-Braintree';
                ELSEIF red_is_ashmont_branch(NEW.trip_id, NEW."timestamp") THEN
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
    op.execute(downgrade_branch_trunk_function)

    alter_view = 'ALTER VIEW static_service_id_lookup RENAME COLUMN static_version_key TO "timestamp";'
    op.execute(alter_view)
    downgrade_view = """
        CREATE OR REPLACE VIEW static_service_id_lookup AS 
        WITH mod_feed_dates AS (
            SELECT 
                feed_start_date::text::date AS start_date,
                feed_end_date::text::date AS end_date,
                "timestamp"
            FROM public.static_feed_info
        ), sd_match AS (
            SELECT 
                replace(mod_sd::date::text, '-', '')::integer AS service_date,
                mod_feed_dates."timestamp"
            FROM mod_feed_dates,
                generate_series(mod_feed_dates.start_date, mod_feed_dates.end_date, '1 day') AS mod_sd
        ), static_cal AS (
            SELECT
                service_id, 
                start_date::text::date AS start_date, 
                end_date::text::date AS end_date,
                "timestamp",
                monday,
                tuesday,
                wednesday,
                thursday,
                friday,
                saturday,
                sunday
            FROM 
                public.static_calendar
            WHERE 
                public.static_calendar."timestamp" >= (SELECT min("timestamp") FROM public.static_calendar_dates)
        ), service_ids AS (
            SELECT
                static_cal.service_id,
                replace(new_sd::date::text, '-', '')::integer AS service_date,
                static_cal."timestamp" as "timestamp"
            FROM static_cal,
                generate_series(static_cal.start_date, static_cal.end_date, '1 day') AS new_sd
            WHERE 
                (extract(dow from new_sd) = 0 AND static_cal.sunday)
                OR (extract(dow from new_sd) = 1 AND static_cal.monday)
                OR (extract(dow from new_sd) = 2 AND static_cal.tuesday)
                OR (extract(dow from new_sd) = 3 AND static_cal.wednesday)
                OR (extract(dow from new_sd) = 4 AND static_cal.thursday)
                OR (extract(dow from new_sd) = 5 AND static_cal.friday)
                OR (extract(dow from new_sd) = 6 AND static_cal.saturday)
        ), service_ids_special AS (
            SELECT 
                scd.service_id,
                scd."date" AS service_date,
                scd."timestamp"
            FROM 
                public.static_calendar_dates scd
            WHERE 
                exception_type = 1
        ), service_ids_exclude AS (
            SELECT 
                scd.service_id,
                scd."date" AS service_date,
                scd."timestamp",
                true AS to_exclude
            FROM 
                public.static_calendar_dates scd
            WHERE 
                exception_type = 2
        ), all_service_ids AS (
            SELECT 
                service_id,
                service_date,
                "timestamp"
            FROM 
                service_ids
            UNION
            SELECT
                service_id,
                service_date,
                "timestamp"
            FROM 
                service_ids_special
        )
        SELECT
            st.route_id,
            a_sid.service_id,
            a_sid.service_date,
            a_sid."timestamp"
        FROM 
            all_service_ids a_sid
        FULL OUTER JOIN
            service_ids_exclude e_sid
        ON 
            a_sid.service_id = e_sid.service_id
            AND a_sid.service_date = e_sid.service_date
            AND a_sid."timestamp" = e_sid."timestamp"
        JOIN sd_match
        ON
            sd_match."timestamp" = a_sid."timestamp"
            AND sd_match.service_date = a_sid.service_date
        JOIN 
            public.static_trips st 
        ON
            a_sid.service_id = st.service_id 
            AND a_sid."timestamp" = st."timestamp"
        WHERE
            e_sid.to_exclude IS null
        GROUP BY
            st.route_id,
            a_sid.service_id,
            a_sid.service_date,
            a_sid."timestamp"
        ;
    """
    op.execute(downgrade_view)

    alter_view = 'ALTER VIEW service_id_by_date_and_route RENAME COLUMN static_version_key TO "timestamp";'
    op.execute(alter_view)
    downgrade_view = """
        CREATE OR REPLACE VIEW service_id_by_date_and_route AS 
        WITH mod_feed_dates AS (
            SELECT 
                feed_start_date::text::date AS start_date,
                lead(feed_start_date,1,replace(CURRENT_DATE::text, '-', '')::integer) OVER (ORDER BY feed_start_date)::text::date - 1 AS end_date,
                "timestamp"
            FROM public.static_feed_info
        ), sd_match AS (
            SELECT 
                replace(mod_sd::date::text, '-', '')::integer AS service_date,
                mod_feed_dates."timestamp"
            FROM mod_feed_dates,
                generate_series(mod_feed_dates.start_date, mod_feed_dates.end_date, '1 day') AS mod_sd
        ), static_cal AS (
            SELECT
                service_id, 
                start_date::text::date AS start_date, 
                end_date::text::date AS end_date,
                "timestamp",
                monday,
                tuesday,
                wednesday,
                thursday,
                friday,
                saturday,
                sunday
            FROM 
                public.static_calendar
            WHERE 
                public.static_calendar."timestamp" >= (SELECT min("timestamp") FROM public.static_calendar_dates)
        ), service_ids AS (
            SELECT
                static_cal.service_id,
                replace(new_sd::date::text, '-', '')::integer AS service_date,
                static_cal."timestamp" as "timestamp"
            FROM static_cal,
                generate_series(static_cal.start_date, static_cal.end_date, '1 day') AS new_sd
            WHERE 
                (extract(dow from new_sd) = 0 AND static_cal.sunday)
                OR (extract(dow from new_sd) = 1 AND static_cal.monday)
                OR (extract(dow from new_sd) = 2 AND static_cal.tuesday)
                OR (extract(dow from new_sd) = 3 AND static_cal.wednesday)
                OR (extract(dow from new_sd) = 4 AND static_cal.thursday)
                OR (extract(dow from new_sd) = 5 AND static_cal.friday)
                OR (extract(dow from new_sd) = 6 AND static_cal.saturday)
        ), service_ids_special AS (
            SELECT 
                scd.service_id,
                scd."date" AS service_date,
                scd."timestamp"
            FROM 
                public.static_calendar_dates scd
            WHERE 
                exception_type = 1
        ), service_ids_exclude AS (
            SELECT 
                scd.service_id,
                scd."date" AS service_date,
                scd."timestamp",
                true AS to_exclude
            FROM 
                public.static_calendar_dates scd
            WHERE 
                exception_type = 2
        ), all_service_ids AS (
            SELECT 
                service_id,
                service_date,
                "timestamp"
            FROM 
                service_ids
            UNION
            SELECT
                service_id,
                service_date,
                "timestamp"
            FROM 
                service_ids_special
        )
        SELECT
            st.route_id,
            a_sid.service_id,
            a_sid.service_date,
            a_sid."timestamp"
        FROM 
            all_service_ids a_sid
        FULL OUTER JOIN
            service_ids_exclude e_sid
        ON 
            a_sid.service_id = e_sid.service_id
            AND a_sid.service_date = e_sid.service_date
            AND a_sid."timestamp" = e_sid."timestamp"
        JOIN sd_match
        ON
            sd_match."timestamp" = a_sid."timestamp"
            AND sd_match.service_date = a_sid.service_date
        JOIN 
            public.static_trips st 
        ON
            a_sid.service_id = st.service_id 
            AND a_sid."timestamp" = st."timestamp"
        WHERE
            e_sid.to_exclude IS null
            AND st.route_id IN ('Red', 'Mattapan', 'Orange', 'Green-B', 'Green-C', 'Green-D', 'Green-E', 'Blue')
        GROUP BY
            st.route_id,
            a_sid.service_id,
            a_sid.service_date,
            a_sid."timestamp"
        ;
    """
    op.execute(downgrade_view)
