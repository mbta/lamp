func_insert_feed_info = r"""
        CREATE OR REPLACE FUNCTION insert_feed_info() RETURNS TRIGGER AS $$ 
        BEGIN 
            IF NEW.feed_version ~ '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}' THEN
                NEW.feed_active_date := replace((substring(NEW.feed_version from '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2}')::timestamptz at time zone 'US/Eastern')::date::text,'-','')::integer;
            ELSE
                NEW.feed_active_date := NEW.feed_start_date;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    """


ashmond_stop_ids = "('70087', '70088', '70089', '70090', '70091', '70092', '70093', '70094', '70085', '70086')"


braintree_stop_ids = (
    "('70097', '70098', '70099', '70100', '70101', '70102', '70103', '70104', '70105', '70095', '70096')"
)


func_red_is_ashmont_branch = f"""
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
                AND sst.stop_id IN {ashmond_stop_ids}
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


func_red_is_braintree_branch = f"""
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
                AND sst.stop_id IN {braintree_stop_ids}
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


func_static_trips_branch_trunk = """
        CREATE OR REPLACE FUNCTION insert_static_trips_branch_trunk() RETURNS TRIGGER AS $$ 
        BEGIN 
            IF NEW.route_id ~ 'Green-' THEN
                NEW.branch_route_id := NEW.route_id;
                NEW.trunk_route_id := 'Green';
            ELSEIF NEW.route_id = 'Red' THEN
                IF red_is_braintree_branch(NEW.trip_id, NEW.static_version_key) THEN
                    NEW.branch_route_id := 'Red-B';
                ELSEIF red_is_ashmont_branch(NEW.trip_id, NEW.static_version_key) THEN
                    NEW.branch_route_id := 'Red-A';
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

func_rt_red_is_braintree_branch = f"""
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
                AND ve.stop_id IN {braintree_stop_ids}
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


func_rt_red_is_ashmont_branch = f"""
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
                AND ve.stop_id IN {ashmond_stop_ids}
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


func_rt_trips_branch_trunk = """
        CREATE OR REPLACE FUNCTION update_rt_branch_trunk_id() RETURNS TRIGGER AS $$ 
        BEGIN 
            IF NEW.route_id ~ 'Green-' THEN
                NEW.branch_route_id := NEW.route_id;
                NEW.trunk_route_id := 'Green';
            ELSEIF NEW.route_id = 'Red' THEN
                IF rt_red_is_braintree_branch(NEW.pm_trip_id) THEN
                    NEW.branch_route_id := 'Red-B';
                ELSEIF rt_red_is_ashmont_branch(NEW.pm_trip_id) THEN
                    NEW.branch_route_id := 'Red-A';
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


view_service_id_by_date_and_route = """
        CREATE OR REPLACE VIEW service_id_by_date_and_route AS 
        SELECT 
                static_trips.route_id,
                all_service_ids.service_id,
                all_service_ids.service_date,
                all_service_ids.static_version_key
        FROM
        (
                SELECT 
                        service_ids.service_id AS service_id,
                        service_ids.service_date AS service_date,
                        service_ids.static_version_key AS static_version_key
                FROM
                (
                        SELECT 
                                static_cal.service_id AS service_id,
                                TO_CHAR(new_sd, 'YYYYMMDD')::integer AS service_date,
                                static_cal.static_version_key AS static_version_key
                        FROM
                        (
                                SELECT 
                                        static_calendar.service_id AS service_id,
                                        start_date::text::date AS start_date,
                                        end_date::text::date AS end_date,
                                        static_calendar.static_version_key AS static_version_key,
                                        static_calendar.monday AS monday,
                                        static_calendar.tuesday AS tuesday,
                                        static_calendar.wednesday AS wednesday,
                                        static_calendar.thursday AS thursday,
                                        static_calendar.friday AS friday,
                                        static_calendar.saturday AS saturday,
                                        static_calendar.sunday AS sunday
                                FROM static_calendar
                                WHERE 
                                        static_calendar.static_version_key >= (SELECT min(static_calendar_dates.static_version_key) FROM static_calendar_dates)
                        ) AS static_cal,
                        generate_series(static_cal.start_date, static_cal.end_date, '1 day') AS new_sd
                        WHERE 
                                (extract(dow from new_sd) = 0 AND static_cal.sunday)
                                OR (extract(dow from new_sd) = 1 AND static_cal.monday)
                                OR (extract(dow from new_sd) = 2 AND static_cal.tuesday)
                                OR (extract(dow from new_sd) = 3 AND static_cal.wednesday)
                                OR (extract(dow from new_sd) = 4 AND static_cal.thursday)
                                OR (extract(dow from new_sd) = 5 AND static_cal.friday)
                                OR (extract(dow from new_sd) = 6 AND static_cal.saturday)
                ) AS service_ids
                UNION 
                SELECT 
                        service_ids_special.service_id AS service_id,
                        service_ids_special.service_date AS service_date,
                        service_ids_special.static_version_key AS static_version_key
                FROM
                (
                        SELECT 
                                static_calendar_dates.service_id AS service_id,
                                static_calendar_dates.date AS service_date,
                                static_calendar_dates.static_version_key AS static_version_key
                        FROM static_calendar_dates
                        WHERE 
                                static_calendar_dates.exception_type = 1
                ) AS service_ids_special
        ) AS all_service_ids
        FULL OUTER JOIN
        (
                SELECT 
                        static_calendar_dates.service_id AS service_id,
                        static_calendar_dates.date AS service_date,
                        static_calendar_dates.static_version_key AS static_version_key,
                        true AS to_exclude
                FROM static_calendar_dates
                WHERE 
                        static_calendar_dates.exception_type = 2
        ) AS service_ids_exclude 
        ON 
                all_service_ids.service_id = service_ids_exclude.service_id
                AND service_ids_exclude.service_date = all_service_ids.service_date
                AND all_service_ids.static_version_key = service_ids_exclude.static_version_key
        JOIN
        (
                SELECT 
                        TO_CHAR(mod_sd, 'YYYYMMDD')::integer AS service_date,
                        static_version_key
                FROM
                (
                        SELECT 
                                feed_start_date::text::date AS start_date,
                                lead(feed_start_date, 1, feed_end_date) OVER (ORDER BY feed_start_date)::text::date - 1 AS end_date,
                                static_version_key
                        FROM public.static_feed_info
                ) AS mod_fed_dates,
                generate_series(mod_fed_dates.start_date, mod_fed_dates.end_date, '1 day') AS mod_sd
        ) AS sd_match 
        ON 
                all_service_ids.static_version_key = sd_match.static_version_key
                AND all_service_ids.service_date = sd_match.service_date
        JOIN 
                static_trips 
        ON 
                all_service_ids.service_id = static_trips.service_id
                AND all_service_ids.static_version_key = static_trips.static_version_key
        WHERE 
                service_ids_exclude.to_exclude IS NULL
                AND static_trips.route_id IN ('Red', 'Mattapan', 'Orange', 'Green-B', 'Green-C', 'Green-D', 'Green-E', 'Blue')
        GROUP BY 
                static_trips.route_id,
                all_service_ids.service_id,
                all_service_ids.service_date,
                all_service_ids.static_version_key
        ;
    """


view_static_service_id_lookup = """
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


view_opmi_all_rt_fields_joined = """
        CREATE OR REPLACE VIEW opmi_all_rt_fields_joined AS 
        SELECT
            vt.service_date
            , ve.pm_trip_id
            , ve.stop_sequence
            , ve.canonical_stop_sequence
            , prev_ve.canonical_stop_sequence as previous_canonical_stop_sequence
            , ve.sync_stop_sequence
            , prev_ve.sync_stop_sequence as previous_sync_stop_sequence
            , ve.stop_id
            , prev_ve.stop_id as previous_stop_id
            , ve.parent_station
            , prev_ve.parent_station as previous_parent_station
            , ve.vp_move_timestamp as previous_stop_departure_timestamp
            , COALESCE(ve.vp_stop_timestamp,  ve.tu_stop_timestamp) as stop_arrival_timestamp
            , COALESCE(ve.vp_stop_timestamp,  ve.tu_stop_timestamp) + ve.dwell_time_seconds as stop_departure_timestamp
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
        LEFT JOIN
            vehicle_events prev_ve
        ON
            ve.pm_event_id = prev_ve.next_trip_stop_pm_event_id
        WHERE
            ve.previous_trip_stop_pm_event_id is not NULL 
            AND ( 
                ve.vp_stop_timestamp IS NOT null
                OR ve.vp_move_timestamp IS NOT null 
            )
            
        ;
    """
