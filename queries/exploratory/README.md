# Exploratory SQL (not run by Airflow)

SQL files here are **manual / learning** queries only. They are **not** referenced by:

- `dags/marketlens_daily.py`
- `start.sh` or ingestion scripts

Run them in **Snowsight** (or your SQL client) against `SCORPION_DB.MARKETLENS` after selecting the right role/warehouse.

Use them to understand table shapes, join patterns, and how signals relate to raw data — without changing the production pipeline.
