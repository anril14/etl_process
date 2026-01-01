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
   - Запись с информацией о файле в `registry` таблицу
   - Очистка и валидация данных через in_memory таблицы в duckdb
   - Идемпотентность достигается засчет флага обработки файла в `registry` таблице
   - Сохранение только валидных данных, разделенных на 'complete' и 'quarantine' (не попавшие в выборку)
   - Загрузка батчами в ods-слой в бд
3. **ODS (Operational Data Store)**: Postgres
   - Таблицы:
     - `taxi_data` - корректные данные
     - `taxi_data_quarantine` - подозрительные записи (не сходящиеся по дате, с несуществующими внешними id в справочных таблицах)
     - 'quarantine' данные сохраняются для последующего анализа и улучшения правил валидации
     - Справочные таблицы (`vendor`, `ratecode`, `payment`)
   - Constraints на справочные таблицы
4. **Data Mart**: Metabase
   - Таблицы:
     - `taxi_data` - таблица с демонстрационными данными
   - Конструируемый дашборд по данным из таблицы в metabase:
<img width="863" height="717" alt="image" src="https://github.com/user-attachments/assets/7f3e8f1a-cca0-4a1c-9a8f-41139ffa5f3d" />

## Список DAG'ов Airflow:

### Запуск DAG'ов производится вручную
 - **save_raw_data_to_minio**
 Сохранение данных из API
 
 - **process_data_into_ods**
 Загрузка данных в ods-слой
 
 - **recalculate_data_mart**
 Пересчет data mart-слоя

 - **sql_migrations**
 Загрузка всех миграций sql
 (На случай если не сработают автоматические при первом запуске контейнеров)

 - **d_save_raw_data**
 Устаревший вариант сохранения данных из API
 (Для демонстрации разницы в скорости работы)

 - **d_process_data_into_ods**
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
```env
# Airflow
AIRFLOW_UID=1000
AIRFLOW__CORE__LOAD_EXAMPLES=false

# Openweather
OPENWEATHER_API_KEY=OPENWEATHER_API_KEY

# MinIO
MINIO_ROOT_USER=MINIO_ROOT_USER
MINIO_ROOT_PASSWORD=MINIO_ROOT_PASSWORD
MINIO_ACCESS_KEY=MINIO_ACCESS_KEY
MINIO_SECRET_KEY=MINIO_SECRET_KEY
MINIO_ENDPOINT=minio:9000
MINIO_BUCKET_NAME=dev


# DWH PostgreSQL
POSTGRES_DWH_HOST=postgres_dwh
POSTGRES_DWH_PORT=5432
POSTGRES_DWH_DB=postgres
POSTGRES_DWH_USER=postgres
POSTGRES_DWH_PASSWORD=postgres

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

3. Загрузка переменных в Airflow
variables.json -> Airflow
<img width="218" height="226" alt="image" src="https://github.com/user-attachments/assets/e4b624f4-fc4b-4c35-95c8-7cf95f13afee" />

4. Схемы SQL
```sql
create schema if not exists stg;
create schema if not exists reg;
create schema if not exists ods;
create schema if not exists dm;

comment on schema reg is 'Registry';
comment on schema stg is 'Staging';
comment on schema ods is 'Operational data store';
comment on schema dm is 'Data marts';
```

```sql
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
```

```sql
create index if not exists idx_reg_сovered_dates on reg.taxi_data (covered_dates);
```

```sql
create table if not exists stg.taxi_data (
    id serial primary key,
    vendor_id varchar,
    tpep_pickup varchar,
    tpep_dropoff varchar,
    passenger_count varchar,
    trip_distance varchar,
    ratecode_id varchar,
    store_and_forward varchar,
    pu_location_id varchar,
    do_location_id varchar,
    payment_type varchar,
    fare varchar,
    extras varchar,
    mta_tax varchar,
    tip varchar,
    tolls varchar,
    improvement varchar,
    total varchar,
    congestion varchar,
    airport_fee varchar,
    cbd_congestion_fee varchar
    );
```

```sql
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
	airport_fee numeric(12,2) default 0.0,
	cbd_congestion_fee numeric(12,2) default 0.0,
	source_system varchar(50) default 'TLC Taxi'
	);

create table if not exists ods.taxi_data_quarantine (
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
	airport_fee numeric(12,2) default 0.0,
	cbd_congestion_fee numeric(12,2) default 0.0,
	source_system varchar(50) default 'TLC Taxi'
	);
```

```sql
insert into ods.vendor(id, description)
	values (1, 'Creative Mobile Technologies, LLC'),
		(2, 'Curb Mobility, LLC'),
		(6, 'Myle Technologies Inc'),
		(7, 'Helix');

insert into ods.ratecode(id, description)
	values (1, 'Standard rate'),
		(2, 'JFK'),
		(3, 'Newark'),
		(4, 'Nassau or Westchester'),
		(5, 'Negotiated fare'),
		(6, 'Group ride'),
		(99, 'Null/unknown');

insert into ods.payment(id, description)
	values (0, 'Flex Fare trip'),
		(1, 'Credit card'),
		(2, 'Cash'),
		(3, 'No charge'),
		(4, 'Dispute'),
		(5, 'Unknown'),
		(6, 'Voided trip');
```

```sql
alter table ods.taxi_data
add constraint fk__taxi__data_vendor_id__vendor__id foreign key (vendor_id) references ods.vendor(id)
		on delete no action,
add constraint fk__taxi__ratecode_id__ratecode__id foreign key (ratecode_id) references ods.ratecode(id)
		on delete no action,
add constraint fk__taxi__payment_type__payment__id foreign key (payment_type) references ods.payment(id)
		on delete no action;
```

```sql
create table if not exists dm.daily_metrics(
	date timestamp primary key,
	avg_total numeric(12, 2) not null,
	avg_tip numeric(12, 2) not null,
    avg_passenger_count numeric(12, 2) not null,
    avg_trip_distance numeric(12,2) not null
	);
```
