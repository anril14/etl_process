create table if not exists stg.weather_data (
	id_measure serial primary key,
	raw_path varchar(255) not null,
	load_time timestamp default current_timestamp,
	dt bigint not null,
	city varchar(100) not null,
	source varchar(100) default 'api.openweathermap.org'

--	constraint unique_dt_city unique (dt, city)
	);

--create index if not exists idx_stg_dt_city on stg.weather_data (dt, city)