create table if not exists ods.vendor(
	id smallint primary key,
	description varchar(50) not null
	);

create table if not exists ods.ratecode(
	id smallint primary key,
	description varchar(50) not null
	);

create table if not exists ods.payment(
	id smallint primary key,
	description varchar(50) not null
	);

create table if not exists ods.taxi_data (
	id serial primary key,
	vendor_id smallint not null,
	tpep_pickup timestamp not null,
	tpep_dropoff timestamp not null,
	passenger_count smallint not null,
	trip_distance numeric(12,2) not null,
	ratecode_id smallint default 99,
	store_and_forward boolean not null,
	pu_location_id smallint,
	do_location_id smallint,
	payment_type smallint not null,
	fare numeric(12,2) not null,
	extras numeric(12,2) not null,
	mta_tax numeric(12,2) not null,
	tip numeric(12,2) not null,
	tolls numeric(12,2) not null,
	improvement numeric(12,2) not null,
	total numeric(12,2) not null,
	congestion numeric(12,2) not null,
	airport_fee numeric(12,2) not null default 0.0,
	cbd_congestion_fee numeric(12,2) not null default 0.0,
	source_system varchar(50) default 'TLC Taxi'
	);