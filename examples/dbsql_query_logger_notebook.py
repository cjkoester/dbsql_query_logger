# Databricks notebook source
dbutils.widgets.text('catalog', '', '01 Catalog')
dbutils.widgets.text('schema', '', '02 Schema')
dbutils.widgets.text('table', 'query_history', '03 Table')
dbutils.widgets.dropdown('pipeline_mode', 'triggered', ['triggered', 'continuous'], '04 Pipeline Mode')
dbutils.widgets.dropdown('backfill_period', '7 days', ['1 day', '7 days', '14 days', '21 days', '30 days'], '05 Reset')
dbutils.widgets.dropdown('reset', 'no', ['yes', 'no'], '06 Reset')

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.18.0 --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from dbsql_query_logger import QueryLogger

query_logger = QueryLogger(
    catalog = dbutils.widgets.get('catalog'),
    schema = dbutils.widgets.get('schema'),
    table = dbutils.widgets.get('table'),
    pipeline_mode = dbutils.widgets.get('pipeline_mode'),
    backfill_period = dbutils.widgets.get('backfill_period'),
    reset = dbutils.widgets.get('reset')
)

query_logger.run()
