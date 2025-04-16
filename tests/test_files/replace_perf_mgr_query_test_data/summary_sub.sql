		SELECT
			static_trips_sub.static_trip_id AS static_trip_id,
			COALESCE(static_trips_sub.branch_route_id,
			static_trips_sub.trunk_route_id) AS route_id,
			static_trips_sub.direction_id AS direction_id,
			first_stop_static_sub.static_start_time AS static_start_time,
			static_trips_sub.static_trip_stop_rank AS static_stop_count
		FROM
			(
			SELECT
				static_stop_times.static_version_key AS static_version_key,
				static_stop_times.trip_id AS static_trip_id,
				static_stop_times.arrival_time AS static_stop_timestamp,
				COALESCE(static_stops.parent_station,
				static_stops.stop_id) AS parent_station,
				lag(static_stop_times.departure_time) OVER (PARTITION BY static_stop_times.static_version_key,
				static_stop_times.trip_id
			ORDER BY
				static_stop_times.stop_sequence) IS NULL AS static_trip_first_stop,
				lead(static_stop_times.departure_time) OVER (PARTITION BY static_stop_times.static_version_key,
				static_stop_times.trip_id
			ORDER BY
				static_stop_times.stop_sequence) IS NULL AS static_trip_last_stop,
				RANK() OVER (PARTITION BY static_stop_times.static_version_key,
				static_stop_times.trip_id
			ORDER BY
				static_stop_times.stop_sequence) AS static_trip_stop_rank,
				static_trips.route_id AS route_id,
				static_trips.branch_route_id AS branch_route_id,
				static_trips.trunk_route_id AS trunk_route_id,
				static_trips.direction_id AS direction_id
			FROM
				static_stop_times
			JOIN static_trips ON
				static_stop_times.static_version_key = static_trips.static_version_key
				AND static_stop_times.trip_id = static_trips.trip_id
			JOIN static_stops ON
				static_stop_times.static_version_key = static_stops.static_version_key
				AND static_stop_times.stop_id = static_stops.stop_id
			JOIN static_service_id_lookup ON
				static_stop_times.static_version_key = static_service_id_lookup.static_version_key
				AND static_trips.service_id = static_service_id_lookup.service_id
				AND static_trips.route_id = static_service_id_lookup.route_id
			JOIN static_routes ON
				static_stop_times.static_version_key = static_routes.static_version_key
				AND static_trips.route_id = static_routes.route_id
			WHERE
				static_stop_times.static_version_key = 1744126668
				AND static_trips.static_version_key = 1744126668
				AND static_stops.static_version_key = 1744126668
				AND static_service_id_lookup.static_version_key = 1744126668
				AND static_routes.static_version_key = 1744126668
				AND static_routes.route_type != 3
				AND static_service_id_lookup.service_date = 20250410) AS static_trips_sub
		JOIN (
			SELECT
				static_trips_sub.static_trip_id AS static_trip_id,
				static_trips_sub.static_stop_timestamp AS static_start_time
			FROM
				(
				SELECT
					static_stop_times.static_version_key AS static_version_key,
					static_stop_times.trip_id AS static_trip_id,
					static_stop_times.arrival_time AS static_stop_timestamp,
					COALESCE(static_stops.parent_station,
					static_stops.stop_id) AS parent_station,
					lag(static_stop_times.departure_time) OVER (PARTITION BY static_stop_times.static_version_key,
					static_stop_times.trip_id
				ORDER BY
					static_stop_times.stop_sequence) IS NULL AS static_trip_first_stop,
					lead(static_stop_times.departure_time) OVER (PARTITION BY static_stop_times.static_version_key,
					static_stop_times.trip_id
				ORDER BY
					static_stop_times.stop_sequence) IS NULL AS static_trip_last_stop,
					RANK() OVER (PARTITION BY static_stop_times.static_version_key,
					static_stop_times.trip_id
				ORDER BY
					static_stop_times.stop_sequence) AS static_trip_stop_rank,
					static_trips.route_id AS route_id,
					static_trips.branch_route_id AS branch_route_id,
					static_trips.trunk_route_id AS trunk_route_id,
					static_trips.direction_id AS direction_id
				FROM
					static_stop_times
				JOIN static_trips ON
					static_stop_times.static_version_key = static_trips.static_version_key
					AND static_stop_times.trip_id = static_trips.trip_id
				JOIN static_stops ON
					static_stop_times.static_version_key = static_stops.static_version_key
					AND static_stop_times.stop_id = static_stops.stop_id
				JOIN static_service_id_lookup ON
					static_stop_times.static_version_key = static_service_id_lookup.static_version_key
					AND static_trips.service_id = static_service_id_lookup.service_id
					AND static_trips.route_id = static_service_id_lookup.route_id
				JOIN static_routes ON
					static_stop_times.static_version_key = static_routes.static_version_key
					AND static_trips.route_id = static_routes.route_id
				WHERE
					static_stop_times.static_version_key = 1744126668
					AND static_trips.static_version_key = 1744126668
					AND static_stops.static_version_key = 1744126668
					AND static_service_id_lookup.static_version_key = 1744126668
					AND static_routes.static_version_key = 1744126668
					AND static_routes.route_type != 3
					AND static_service_id_lookup.service_date = 20250410) AS static_trips_sub
			WHERE
				static_trips_sub.static_trip_first_stop = TRUE) AS first_stop_static_sub ON
			static_trips_sub.static_trip_id = first_stop_static_sub.static_trip_id
		WHERE
			static_trips_sub.static_trip_last_stop = TRUE
        ORDER BY 
            static_trip_id