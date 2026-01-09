CREATE TABLE IF NOT EXISTS ods.vendor(
	id SMALLINT PRIMARY KEY,
	description VARCHAR(50) NOT NULL
	);

CREATE TABLE IF NOT EXISTS ods.ratecode(
	id SMALLINT PRIMARY KEY,
	description VARCHAR(50) NOT NULL
	);

CREATE TABLE IF NOT EXISTS ods.payment(
	id SMALLINT PRIMARY KEY,
	description VARCHAR(50) NOT NULL
	);

CREATE TABLE IF NOT EXISTS ods.taxi_data (
	id serial PRIMARY KEY,
	vendor_id SMALLINT NOT NULL,
	tpep_pickup TIMESTAMP NOT NULL,
	tpep_dropoff TIMESTAMP NOT NULL,
	passenger_count SMALLINT NOT NULL,
	trip_distance NUMERIC(12,2) NOT NULL,
	ratecode_id SMALLINT DEFAULT 99,
	store_and_forward BOOLEAN NOT NULL,
	pu_location_id SMALLINT,
	do_location_id SMALLINT,
	payment_type SMALLINT NOT NULL,
	fare NUMERIC(12,2) NOT NULL,
	extras NUMERIC(12,2) NOT NULL,
	mta_tax NUMERIC(12,2) NOT NULL,
	tip NUMERIC(12,2) NOT NULL,
	tolls NUMERIC(12,2) NOT NULL,
	improvement NUMERIC(12,2) NOT NULL,
	total NUMERIC(12,2) NOT NULL,
	congestion NUMERIC(12,2) NOT NULL,
	airport_fee NUMERIC(12,2) DEFAULT 0.0,
	cbd_congestion_fee NUMERIC(12,2) DEFAULT 0.0,
	source_system VARCHAR(50) DEFAULT 'TLC Taxi'
	);

CREATE TABLE IF NOT EXISTS ods.taxi_data_quarantine (
	id serial PRIMARY KEY,
	vendor_id SMALLINT NOT NULL,
	tpep_pickup TIMESTAMP NOT NULL,
	tpep_dropoff TIMESTAMP NOT NULL,
	passenger_count SMALLINT NOT NULL,
	trip_distance NUMERIC(12,2) NOT NULL,
	ratecode_id SMALLINT DEFAULT 99,
	store_and_forward BOOLEAN NOT NULL,
	pu_location_id SMALLINT,
	do_location_id SMALLINT,
	payment_type SMALLINT NOT NULL,
	fare NUMERIC(12,2) NOT NULL,
	extras NUMERIC(12,2) NOT NULL,
	mta_tax NUMERIC(12,2) NOT NULL,
	tip NUMERIC(12,2) NOT NULL,
	tolls NUMERIC(12,2) NOT NULL,
	improvement NUMERIC(12,2) NOT NULL,
	total NUMERIC(12,2) NOT NULL,
	congestion NUMERIC(12,2) NOT NULL,
	airport_fee NUMERIC(12,2) DEFAULT 0.0,
	cbd_congestion_fee NUMERIC(12,2) DEFAULT 0.0,
	source_system VARCHAR(50) DEFAULT 'TLC Taxi'
	);