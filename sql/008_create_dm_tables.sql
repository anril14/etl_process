create table if not exists dm.daily_metrics(
	date timestamp primary key,
	avg_total numeric(12, 2) not null,
	avg_tip numeric(12, 2) not null,
    avg_passenger_count numeric(12, 2) not null,
    avg_trip_distance numeric(12,2) not null
	);
