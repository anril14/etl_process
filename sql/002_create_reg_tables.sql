CREATE TABLE IF NOT EXISTS reg.taxi_data (
	id_measure serial PRIMARY KEY,
	raw_path VARCHAR(255) NOT NULL,
	covered_dates VARCHAR(50) NOT NULL,
	load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	source VARCHAR(100) DEFAULT 'https://d37ci6vzurychx.cloudfront.net',
	processed boolean DEFAULT FALSE,
	processed_time TIMESTAMP DEFAULT NULL,
	file_size bigint NOT NULL
	);