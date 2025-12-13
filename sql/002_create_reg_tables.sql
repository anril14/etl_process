create table if not exists reg.taxi_data (
	id_measure serial primary key,
	raw_path varchar(255) not null,
	covered_dates varchar(50) not null,
	load_time timestamp default current_timestamp,
	source varchar(100) default 'https://d37ci6vzurychx.cloudfront.net',
	processed boolean default false,
	processed_time timestamp default null,
	file_size bigint not null
	);

-- TODO: Подумать над необходимостью "Полноценного" stg-слоя, или попробовать реализовать только т.н. "registry"