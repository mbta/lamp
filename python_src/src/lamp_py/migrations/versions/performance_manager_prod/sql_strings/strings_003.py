view_service_id_by_date_and_route = """
        CREATE OR REPLACE VIEW service_id_by_date_and_route AS 
        SELECT static_trips.route_id,
            all_service_ids.service_id,
            all_service_ids.service_date,
            all_service_ids.static_version_key
        FROM
        (SELECT service_ids.service_id AS service_id,
                service_ids.service_date AS service_date,
                service_ids.static_version_key AS static_version_key
        FROM
            (SELECT static_cal.service_id AS service_id,
                    replace(new_sd::date::text, '-', '')::integer AS service_date,
                    static_cal.static_version_key AS static_version_key
            FROM
                (SELECT static_calendar.service_id AS service_id,
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
                WHERE static_calendar.static_version_key >=
                    (SELECT min(static_calendar_dates.static_version_key) AS min_1
                    FROM static_calendar_dates)) AS static_cal,
                generate_series(static_cal.start_date, static_cal.end_date, '1 day') AS new_sd
            WHERE (extract(dow
                            from new_sd) = 0
                    AND static_cal.sunday)
                OR (extract(dow
                            from new_sd) = 1
                    AND static_cal.monday)
                OR (extract(dow
                            from new_sd) = 2
                    AND static_cal.tuesday)
                OR (extract(dow
                            from new_sd) = 3
                    AND static_cal.wednesday)
                OR (extract(dow
                            from new_sd) = 4
                    AND static_cal.thursday)
                OR (extract(dow
                            from new_sd) = 5
                    AND static_cal.friday)
                OR (extract(dow
                            from new_sd) = 6
                    AND static_cal.saturday)) AS service_ids
        UNION SELECT service_ids_special.service_id AS service_id,
                        service_ids_special.service_date AS service_date,
                        service_ids_special.static_version_key AS static_version_key
        FROM
            (SELECT static_calendar_dates.service_id AS service_id,
                    static_calendar_dates.date AS service_date,
                    static_calendar_dates.static_version_key AS static_version_key
            FROM static_calendar_dates
            WHERE static_calendar_dates.exception_type = 1) AS service_ids_special) AS all_service_ids
        FULL OUTER JOIN
        (SELECT static_calendar_dates.service_id AS service_id,
                static_calendar_dates.date AS service_date,
                static_calendar_dates.static_version_key AS static_version_key,
                true AS to_exclude
        FROM static_calendar_dates
        WHERE static_calendar_dates.exception_type = 2) AS service_ids_exclude ON all_service_ids.service_id = service_ids_exclude.service_id
        AND service_ids_exclude.service_date = all_service_ids.service_date
        AND all_service_ids.static_version_key = service_ids_exclude.static_version_key
        JOIN
        (SELECT replace(mod_sd::date::text, '-', '')::integer AS service_date,
                static_version_key
        FROM
            (SELECT feed_start_date::text::date AS start_date,
                    lead(feed_start_date, 1, feed_end_date) OVER (ORDER BY feed_start_date)::text::date - 1 AS end_date,
                    static_version_key
            FROM public.static_feed_info) AS mod_fed_dates,
                generate_series(mod_fed_dates.start_date, mod_fed_dates.end_date, '1 day') AS mod_sd) AS sd_match ON all_service_ids.static_version_key = sd_match.static_version_key
        AND all_service_ids.service_date = sd_match.service_date
        JOIN static_trips ON all_service_ids.service_id = static_trips.service_id
        WHERE service_ids_exclude.to_exclude IS NULL
        AND static_trips.route_id IN ('Red', 'Mattapan', 'Orange', 'Green-B', 'Green-C', 'Green-D', 'Green-E', 'Blue')
        GROUP BY static_trips.route_id,
                all_service_ids.service_id,
                all_service_ids.service_date,
                all_service_ids.static_version_key
        ;
    """
