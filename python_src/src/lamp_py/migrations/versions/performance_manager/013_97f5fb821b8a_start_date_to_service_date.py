"""start_date to service_date

Revision ID: 97f5fb821b8a
Revises: ced3caaeab23
Create Date: 2023-05-15 14:54:48.190847

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "97f5fb821b8a"
down_revision = "ced3caaeab23"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "vehicle_events", "start_date", new_column_name="service_date"
    )
    op.alter_column(
        "vehicle_trips", "start_date", new_column_name="service_date"
    )

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

    create_view_sql = """
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
    op.execute(create_view_sql)


def downgrade() -> None:
    op.alter_column(
        "vehicle_events", "service_date", new_column_name="start_date"
    )
    op.alter_column(
        "vehicle_trips", "service_date", new_column_name="start_date"
    )
