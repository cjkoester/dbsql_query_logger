# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from query_logger import QueryLogger

query_logger = QueryLogger(
    catalog = dbutils.widgets.get('catalog'),
    schema = dbutils.widgets.get('schema'),
    table = dbutils.widgets.get('table'),
    pipeline_mode = dbutils.widgets.get('pipeline_mode'),
    backfill_period = dbutils.widgets.get('backfill_period'),
    reset = dbutils.widgets.get('reset')
)

query_logger.create_target_table()
query_logger.run()
