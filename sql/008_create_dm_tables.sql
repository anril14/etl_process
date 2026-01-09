CREATE TABLE IF NOT EXISTS dm.daily_metrics(
	date TIMESTAMP PRIMARY KEY,
	avg_total NUMERIC(12, 2) NOT NULL,
	avg_tip NUMERIC(12, 2) NOT NULL,
    avg_passenger_count NUMERIC(12, 2) NOT NULL,
    avg_trip_distance NUMERIC(12,2) NOT NULL
	);