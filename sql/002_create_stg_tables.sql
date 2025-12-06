create table if not exists stg.taxi_data (
	id_measure serial primary key,
	raw_path varchar(255) not null,
	covered_dates varchar(50) not null,
	load_time timestamp default current_timestamp,
	source varchar(100) default 'https://d37ci6vzurychx.cloudfront.net',
	processed boolean default false,
	processed_time timestamp default null,
	file_size bigint not null
	);

create index if not exists idx_stg_сovered_dates on stg.taxi_data (covered_dates);