from lamp_py.performance_manager.l1_cte_statements import static_trips_subquery_pl


static_trips_sub_res = static_trips_subquery_pl(20250415).sort(by="static_trip_id")

static_trips_sub_res.write_csv("20250415_static_trips_subquery.csv")
