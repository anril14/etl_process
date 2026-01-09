# ETL/Data Engineering пайплайн с полной обработкой данных поездок такси

## Цель проекта
Проект демонстрирует:
- построение ETL пайплайна
- работу с большими объемами данных (~3 млн записей за один загружаемый период)
- контроль качества данных (data quality)
- оптимизацию загрузки данных в DWH-хранилище
- визуальную демонстрацию через Metabase

## Архитектура проекта
<img width="1011" height="371" alt="ETL_process" src="https://github.com/user-attachments/assets/6becf4c6-76ac-43c4-8e8f-09c486661c4a" />

1. **Data Lake**: MinIO - Сохранение parquet-файла с данными по поездкам
   - Источник: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page 
   - Описание полей из API: https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf 
2. **ETL**: Apache Airflow, Postgres:
   - Выгрузка данных из parquet файла MinIO
   - Запись с информацией о файле в `registry` таблицу (таблица с информацией о загруженном файле)
   - Очистка и валидация данных через in_memory таблицы в duckdb
   - Идемпотентность достигается засчет флага обработки файла в `registry` таблице
   - Сохранение только валидных данных, разделенных на 'complete' и 'quarantine' (не попавшие в выборку)
   - Загрузка батчами в ods-слой в бд
3. При загрузке не используется stg-слой, так как вся валидация проходит в duckdb и необходимости сохранять все записи нет, данные не прошедшие валидацию по бизнес-логике отдельно заносятся в `taxi_data_quarantine` таблицу
4. **ODS (Operational Data Store)**: Postgres
   - Таблицы:
     - `taxi_data` - корректные ('complete') данные
     - `taxi_data_quarantine` - подозрительные ('quarantine') данные (не сходящиеся по дате, с несуществующими внешними id в справочных таблицах)
     - 'quarantine' данные сохраняются для последующего анализа и улучшения правил валидации
     - Справочные таблицы (`vendor`, `ratecode`, `payment`)
   - Constraints на справочные таблицы
5. **Data Mart**: Metabase
   - Таблицы:
     - `taxi_data` - таблица с демонстрационными данными
   - Список всех таблиц: [Схемы SQL](#схемы-sql)
   - Конструируемый дашборд по данным из таблицы в metabase:
<img width="863" height="717" alt="image" src="https://github.com/user-attachments/assets/7f3e8f1a-cca0-4a1c-9a8f-41139ffa5f3d" />

## Список DAGов Airflow
 - **save_raw_data_to_minio** - 
 Сохранение данных из API -> Вызывает **process_data_into_ods**
 
 - **process_data_into_ods** - 
 Загрузка данных в ods-слой (Запускается автоматически после **save_raw_data_to_minio**) -> Вызывает **recalculate_data_mart**
 
 - **recalculate_data_mart** (Запускается автоматически после **process_data_into_ods**) - 
 Пересчет data mart-слоя

 - **sql_migrations** (Запускается вручную) - 
 Загрузка всех миграций sql
 (На случай если не сработают автоматические при первом запуске контейнеров)

 - **d_save_raw_data** (Запускается вручную) - 
 Устаревший вариант сохранения данных из API
 (Для демонстрации разницы в скорости работы)

 - **d_process_data_into_ods** (Запускается вручную) - 
 Устаревший вариант загрузки данных в ods-слой
 (Для демонстрации разницы в скорости работы)

## Data Quality & Validation

Данные считаются 'complete', если:
- дата поездки соответствует обрабатываемому периоду
- `vendor_id`, `ratecode_id`, `payment_type` существуют в справочниках
- ключевые поля `NOT NULL`

Трансферы данных между таблицами осуществляются с помощью транзакций duckdb (ACID)

Staging таблицы используются dag'ами из папки 'deprecated' (устаревшие)
Не удалены для демонстрации разницы работы в скорости обработки (не используется duckdb)
Пример загрузки в старом формате:
```python
with engine.connect() as conn:
   df_valid.to_sql(
       name='taxi_data',
       schema='ods',
       con=conn,
       if_exists='append',
       index=False,
       method='multi',
       chunksize=batch_size
   )
```
Разница в скорости загрузки (ограничение в 100_000 строк)
Старый подход:
<img width="907" height="133" alt="image" src="https://github.com/user-attachments/assets/042b3874-f822-4c55-ba47-dd452c1b300c" />

Актуальный:
<img width="1031" height="127" alt="image" src="https://github.com/user-attachments/assets/dd074d8f-777f-4b41-a16e-2b4b96325982" />

## Объем данных и скорость обработки

- Загружаемый .parquet файл ~50MB
<img width="532" height="176" alt="image" src="https://github.com/user-attachments/assets/377f7c24-a71a-4964-9320-31c4d893d97f" />

- ~3 000 000 записей за период
- Загрузка в ODS выполняется батчами, в среднем занимает чуть меньше 2-х минут
<img width="1029" height="133" alt="image" src="https://github.com/user-attachments/assets/e95339ca-92c9-4e83-9bd3-b3188b94bfa1" />

## Запуск проекта

1. Создание .env файла в директории проекта

.env.example
```env
# Airflow
AIRFLOW_UID=
AIRFLOW__CORE__LOAD_EXAMPLES=

# Openweather
OPENWEATHER_API_KEY=

# MinIO
MINIO_ROOT_USER=
MINIO_ROOT_PASSWORD=
MINIO_ACCESS_KEY=
MINIO_SECRET_KEY=
MINIO_ENDPOINT=
MINIO_BUCKET_NAME=


# DWH PostgreSQL
POSTGRES_DWH_HOST=
POSTGRES_DWH_PORT=
POSTGRES_DWH_DB=
POSTGRES_DWH_USER=
POSTGRES_DWH_PASSWORD=

# Additional settings
_PIP_ADDITIONAL_REQUIREMENTS=psycopg2-binary pandas minio python-dotenv requests
```

2. Запуск контейнеров docker
```bash
# Сборка контейнеров
docker compose build

# Запуск контейнеров
docker compose up
```

3. Запуск DAGов

[Список DAGов](#список-dagов-airflow)

 - В хронологическом порядке нужно запускать весь процесс с `save_raw_data_to_minio`
 - Этот DAG триггерится автоматически в 00:00 1 числа каждого месяца (по умолчанию данные из API появляются с опозданием примерно в ~2 месяца, поэтому при автоматическом срабатывании считается не текущий месяц, а на 2 раньше).
 - При ручном запуске месяц остается тем же что и в Configuration JSON
 - Чтобы запустить любой DAG (кроме deprecated) для любого месяца и года вручную (в пределах работы API) необходимо в настройках Trigger указать Configuration JSON в формате `configuration_example.json` внутри главной директории проекта

<img width="892" height="846" alt="image" src="https://github.com/user-attachments/assets/f60c6b87-ca55-4fa1-8640-bafe90bea40f" />

## Схемы SQL
```sql
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS reg;
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS dm;

COMMENT ON SCHEMA reg IS 'Registry';
COMMENT ON SCHEMA stg IS 'Staging';
COMMENT ON SCHEMA ods IS 'Operational data store';
COMMENT ON SCHEMA dm IS 'Data marts';
```

```sql
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
```

```sql
CREATE INDEX IF NOT EXISTS idx_reg_сovered_dates ON reg.taxi_data (covered_dates);
```

```sql
# Используется только в deprecated
CREATE TABLE IF NOT EXISTS stg.taxi_data (
    id serial PRIMARY KEY,
    vendor_id VARCHAR,
    tpep_pickup VARCHAR,
    tpep_dropoff VARCHAR,
    passenger_count VARCHAR,
    trip_distance VARCHAR,
    ratecode_id VARCHAR,
    store_and_forward VARCHAR,
    pu_location_id VARCHAR,
    do_location_id VARCHAR,
    payment_type VARCHAR,
    fare VARCHAR,
    extras VARCHAR,
    mta_tax VARCHAR,
    tip VARCHAR,
    tolls VARCHAR,
    improvement VARCHAR,
    total VARCHAR,
    congestion VARCHAR,
    airport_fee VARCHAR,
    cbd_congestion_fee VARCHAR
    );
```

```sql
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
```

```sql
INSERT INTO ods.vendor(id, description)
	VALUES (1, 'Creative Mobile Technologies, LLC'),
		(2, 'Curb Mobility, LLC'),
		(6, 'Myle Technologies Inc'),
		(7, 'Helix');

INSERT INTO ods.ratecode(id, description)
	VALUES (1, 'Standard rate'),
		(2, 'JFK'),
		(3, 'Newark'),
		(4, 'Nassau or Westchester'),
		(5, 'Negotiated fare'),
		(6, 'Group ride'),
		(99, 'Null/unknown');

INSERT INTO ods.payment(id, description)
	VALUES (0, 'Flex Fare trip'),
		(1, 'Credit card'),
		(2, 'Cash'),
		(3, 'No charge'),
		(4, 'Dispute'),
		(5, 'Unknown'),
		(6, 'Voided trip');
```

```sql
ALTER TABLE ods.taxi_data
ADD CONSTRAINT fk__taxi__data_vendor_id__vendor__id FOREIGN KEY (vendor_id) REFERENCES ods.vendor(id)
		ON DELETE NO ACTION,
ADD CONSTRAINT fk__taxi__ratecode_id__ratecode__id FOREIGN KEY (ratecode_id) REFERENCES ods.ratecode(id)
		ON DELETE NO ACTION,
ADD CONSTRAINT fk__taxi__payment_type__payment__id FOREIGN KEY (payment_type) REFERENCES ods.payment(id)
		ON DELETE NO ACTION;
```

```sql
CREATE TABLE IF NOT EXISTS dm.daily_metrics(
	date TIMESTAMP PRIMARY KEY,
	avg_total NUMERIC(12, 2) NOT NULL,
	avg_tip NUMERIC(12, 2) NOT NULL,
    avg_passenger_count NUMERIC(12, 2) NOT NULL,
    avg_trip_distance NUMERIC(12,2) NOT NULL
	);
```
