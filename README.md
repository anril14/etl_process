# ETL/Data Engineering пайплайн с полной обработкой данных поездок такси

## Архитектура проекта

1. **Data Lake**: MinIO - Сохранение parquet-файла с данными по поездкам
Источник: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. **ETL**: Apache Airflow:
   - Выгрузка данных из parquet файла MinIO
   - Очистка и валидация данных через in_memory таблицы в duckdb
   - Сохранение только валидных данных, разделенных на 'complete' и 'quarantine' (не попавшие в выборку)
   - Загрузка батчами в ods-слой в бд
3. **ODS (Operational Data Store)**: Postgres
   - Таблицы:
     - `taxi_data` - корректные данные
     - `taxi_data_quarantine` - подозрительные записи (не сходящиеся по дате, с несуществующими внешними id в справочных таблицах)
     - Справочные таблицы (`vendor`, `ratecode`, `payment`)
   - Constraints на справочные таблицы
4. **Data Mart**: Metabase
   - Таблицы:
     - `taxi_data` - таблица с демонстрационными данными
   - Конструируемый дашборд по данным из таблицы в metabase:
<img width="863" height="717" alt="image" src="https://github.com/user-attachments/assets/7f3e8f1a-cca0-4a1c-9a8f-41139ffa5f3d" />

