"""RT trunk_id and route_id processing

Revision ID: 695ef31623ae
Revises: de55dc40315d
Create Date: 2023-05-05 08:20:37.529875

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "695ef31623ae"
down_revision = "de55dc40315d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "vehicle_trips",
        sa.Column("branch_route_id", sa.String(60), nullable=True),
    )
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

    create_trigger = """
        CREATE TRIGGER rt_trips_update_branch_trunk BEFORE INSERT OR UPDATE ON vehicle_trips 
        FOR EACH ROW EXECUTE PROCEDURE update_rt_branch_trunk_id();
    """
    op.execute(create_trigger)

    update_static_ashmont_func = """
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
    op.execute(update_static_ashmont_func)

    update_static_braintree_func = """
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
    op.execute(update_static_braintree_func)

    update_view = """
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
        ), trips_start_dates AS (
            SELECT
                start_date,
                min(fk_static_timestamp) AS fk_timestamp
            FROM
                public.vehicle_trips
            GROUP BY
                start_date
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
    op.execute(update_view)


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS static_service_id_lookup;")

    drop_trigger = (
        "DROP TRIGGER IF EXISTS rt_trips_update_branch_trunk ON vehicle_trips;"
    )
    op.execute(drop_trigger)

    drop_get_rt_branch_trunk = (
        "DROP function IF EXISTS public.update_rt_branch_trunk_id();"
    )
    op.execute(drop_get_rt_branch_trunk)

    drop_rt_ashmont_func = (
        "DROP function IF EXISTS public.rt_red_is_ashmont_branch();"
    )
    op.execute(drop_rt_ashmont_func)

    drop_rt_braintree_func = (
        "DROP function IF EXISTS public.rt_red_is_braintree_branch();"
    )
    op.execute(drop_rt_braintree_func)

    op.drop_column("vehicle_trips", "branch_route_id")
