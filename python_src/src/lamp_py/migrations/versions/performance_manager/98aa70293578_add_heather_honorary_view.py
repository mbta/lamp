"""add heather honorary view

Revision ID: 98aa70293578
Revises: 2e9d81949da8
Create Date: 2023-04-18 15:30:40.821451

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "98aa70293578"
down_revision = "2e9d81949da8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    create_view_sql = """
        CREATE OR REPLACE VIEW heather_davis_honorary_view AS 
        WITH static_cal AS (
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
                replace(service_date::date::text, '-', '')::integer AS service_date,
                static_cal."timestamp"
            FROM static_cal,
                generate_series(static_cal.start_date, static_cal.end_date, '1 day') AS service_date
            WHERE 
                (extract(dow from service_date) = 0 AND static_cal.sunday)
                OR (extract(dow from service_date) = 1 AND static_cal.monday)
                OR (extract(dow from service_date) = 2 AND static_cal.tuesday)
                OR (extract(dow from service_date) = 3 AND static_cal.wednesday)
                OR (extract(dow from service_date) = 4 AND static_cal.thursday)
                OR (extract(dow from service_date) = 5 AND static_cal.friday)
                OR (extract(dow from service_date) = 6 AND static_cal.saturday)
        ), service_ids_special AS (
            SELECT 
                service_id,
                "date" AS service_date,
                "timestamp"
            FROM 
                public.static_calendar_dates
            WHERE 
                exception_type = 1
        ), service_ids_exclude AS (
            SELECT 
                service_id,
                "date" AS service_date,
                "timestamp",
                true AS to_exclude
            FROM 
                public.static_calendar_dates
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
        JOIN 
            trips_start_dates trip_sd 
        ON
            trip_sd.start_date = a_sid.service_date
            and trip_sd.fk_timestamp = a_sid."timestamp"
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
    drop_view_sql = """
        DROP VIEW IF EXISTS heather_davis_honorary_view;
    """
    op.execute(drop_view_sql)
