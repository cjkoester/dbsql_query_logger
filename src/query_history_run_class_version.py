# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from query_logger import QueryLogger

# COMMAND ----------



catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')
table = dbutils.widgets.get('table')
pipeline_mode = dbutils.widgets.get('pipeline_mode')
backfill_period = dbutils.widgets.get('backfill_period')
reset = dbutils.widgets.get('reset')

query_logger = QueryLogger(
    catalog=catalog,
    schema=schema,
    table=table,
    pipeline_mode=pipeline_mode,
    backfill_period=backfill_period,
    reset=reset
)

#query_logger.create_target_table()

# COMMAND ----------

while True:
    os.system('cls')
    start_time, end_time = get_time_filter(spark, catalog, schema, table, backfill_period)
    query_hist_df = get_query_history(spark, start_time, end_time, include_metrics=True)
    query_hist_parsed_df = parse_query_history(query_hist_df)
    load_query_history(spark, catalog, schema, table, query_hist_parsed_df, start_time)

    if pipeline_mode == 'triggered':
        break

    time.sleep(10)
