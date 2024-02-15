from datetime import datetime, timezone
import time
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType, BooleanType
import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import QueryFilter
from databricks.sdk.service.sql import TimeRange
import itertools
import sys

spark = SparkSession.builder.getOrCreate()

class QueryLogger:
    """Gets DBSQL query history from the Databricks API and merges it into a Delta Lake table.

    Attributes:
        catalog (str): Catalog name
        schema (str): Schema name
        table (str): Table name
        pipeline_mode (str): If set to 'triggered', code will load data and exit. Otherwise it will load data every 10 seconds. 
        backfill_period (str): Controls how far back to look for the initial data load.
        reset (str): If set to 'yes', the target table will be replaced.
    """

    def __init__(self, catalog: str, schema: str, table: str, pipeline_mode: str, backfill_period: str, reset: str):
        self.catalog = catalog
        self.schema = schema
        self.table = table
        self.pipeline_mode = pipeline_mode
        self.backfill_period = backfill_period
        self.reset = reset

    def log(self, message: str):
        """Log with print for now to avoid py4j logger issues
        
        Args:
            message (str): Message to log
        """

        print(f'{datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}: {message}')

    def create_target_table(self):
        """Creates target Delta Lake table"""
    
        create_tbl_stmt = (
            "create or replace table" if self.reset == "yes" else "create table if not exists"
        )
    
        spark.sql(
            f"""
                {create_tbl_stmt} {self.catalog}.{self.schema}.{self.table} (
                    query_id STRING,
                    status STRING,
                    query_text STRING,
                    query_start_time TIMESTAMP,
                    execution_end_time TIMESTAMP,
                    query_end_time TIMESTAMP,
                    user_id BIGINT,
                    user_name STRING,
                    spark_ui_url STRING,
                    warehouse_id STRING,
                    error_message STRING,
                    rows_produced BIGINT,
                    metrics MAP <STRING,STRING>,
                    is_final BOOLEAN,
                    channel_used MAP <STRING,STRING>,
                    duration BIGINT,
                    executed_as_user_id BIGINT,
                    executed_as_user_name STRING,
                    plans_state STRING,
                    statement_type STRING
                )
                using delta cluster by (query_start_time)
                tblproperties (
                  'delta.enableDeletionVectors' = 'true'
                )
            """
        )
        
        self.log(f"Created table {self.catalog}.{self.schema}.{self.table} if it did not already exist.")
    
    def get_time_filter(self):
        """Gets time filters for query history API
        
        To enable incremental loads, the starting time is obtained in the following order:
        1. min(query_start_time) of queries in 'QUEUED' or 'RUNNING' status
        2. max(query_start_time)
        3. current_date() - backfill_period (Initial loads only)

        The end time is the current timestamp.
        
        Returns:
            tuple[datetime, datetime]: start and end timestamps
        """

        start_time_query = f"""
            select
              coalesce(
                (
                  select
                    min(query_start_time) as min_start
                  from
                    {self.catalog}.{self.schema}.{self.table}
                  where
                    status in ('QUEUED', 'RUNNING')
                ),
                max(query_start_time),
                current_date() - interval {self.backfill_period}
              )
            from
              {self.catalog}.{self.schema}.{self.table}
        """
        
        start_time = spark.sql(start_time_query).collect()[0][0]
        end_time = datetime.now(tz=timezone.utc)
        
        self.log(f'API filter start time: {start_time.strftime("%Y-%m-%d %H:%M:%S")}, end time: {end_time.strftime("%Y-%m-%d %H:%M:%S")}')
        return start_time, end_time
    
    def get_query_history(self, start_time, end_time, include_metrics: bool = False):
        """Gets DBSQL query history using the Databricks Python SDK.

        https://docs.databricks.com/api/workspace/queryhistory/list
        
        Args:
            start_time (datetime): Limit results to queries that started after this time.
            end_time (datetime): Limit results to queries that started before this time.
            include_metrics (bool, optional): Whether to include metrics about query. Defaults to False.
        
        Returns:
            generator: generator containing results from the DBSQL query history API
        """
    
        w = WorkspaceClient()
        start_time_ms = int(start_time.timestamp() * 1000)
        end_time_ms = int(end_time.timestamp() * 1000)
    
        query_hist_list = w.query_history.list(
            include_metrics=include_metrics,
            max_results=1000,
            filter_by = QueryFilter(
                query_start_time_range = TimeRange(
                    start_time_ms=start_time_ms, end_time_ms=end_time_ms
                )
            )
        )

        return query_hist_list
    
    def create_dataframe(self, query_hist_list):
        """Creates dataframe from query history API response data.

        Data is minimally processed. Unix timestamps are converted to a human readable datetime.
        
        Args:
            query_hist_list (generator): generator from the Python SDK WorkspaceClient().query_history.list()
        
        Returns:
            DataFrame: DataFrame containing DBSQL query history
        """

        query_hist_list = (i.as_dict() for i in query_hist_list)
        
        df_schema = StructType(
            [
                StructField("query_id", StringType(), True),
                StructField("status", StringType(), True),
                StructField("query_text", StringType(), True),
                StructField("query_start_time_ms", LongType(), True),
                StructField("execution_end_time_ms", LongType(), True),
                StructField("query_end_time_ms", LongType(), True),
                StructField("user_id", LongType(), True),
                StructField("user_name", StringType(), True),
                StructField("spark_ui_url", StringType(), True),
                StructField("warehouse_id", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("rows_produced", LongType(), True),
                StructField("metrics", MapType(StringType(), StringType(), True), True),
                StructField("is_final", BooleanType(), True),
                StructField("channel_used", MapType(StringType(), StringType(), True), True),
                StructField("duration", LongType(), True),
                StructField("executed_as_user_id", LongType(), True),
                StructField("executed_as_user_name", StringType(), True),
                StructField("plans_state", StringType(), True),
                StructField("statement_type", StringType(), True)
            ]
        )
        
        query_hist_df = spark.createDataFrame(query_hist_list, df_schema)

        query_hist_parsed_df = (
            query_hist_df
            .withColumn("query_start_time", F.expr("to_timestamp(query_start_time_ms / 1000)"))
            .withColumn("execution_end_time", F.expr("to_timestamp(execution_end_time_ms / 1000)"))
            .withColumn("query_end_time", F.expr("to_timestamp(query_end_time_ms / 1000)"))
            .drop(*['query_start_time_ms', 'query_end_time_ms', 'execution_end_time_ms'])
        )
        return query_hist_parsed_df
    
    def load_query_history(self, query_hist_parsed_df, start_time):
        """Merges data into a Delta Lake table. Schema evolution is enabled.
        
        The target table is filtered using query_start_time >= '{query_start_time}' to reduce the search space for matches and enable file skipping.
        This optimization is possible because the source data (With the exception of the initial load) will only contain updates for queries that were previously in progress.
        
        Args:
            query_hist_parsed_df (DataFrame): DataFrame containing DBSQL query history
            start_time (int): Unix timestamp in milliseconds
        """

        spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled', True)
        tgt_table = DeltaTable.forName(spark, f'{self.catalog}.{self.schema}.{self.table}')
    
        query_start_time = start_time.strftime("%Y-%m-%d")
        
        merge = (
            tgt_table.alias("t")
            .merge(query_hist_parsed_df.alias("s"), f"t.query_id = s.query_id and t.query_start_time >= '{query_start_time}'")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        self.log(f"Merged data into {self.catalog}.{self.schema}.{self.table}. Target table filtered using query_start_time >= '{query_start_time}'")

    def optimize(self):
        """Optimizes Delta Lake table
        
        The target table uses Liquid Clustering by default. If ZORDER is used, update the optimize command below to include ZORDER columns.
        """

        spark.sql(f'optimize {self.catalog}.{self.schema}.{self.table}')
        self.log(f'Optimized table {self.catalog}.{self.schema}.{self.table}')
    
    def run(self, trigger_interval: int = 30):
        """Runs DBSQL query logger pipeline

        Args:
            trigger_interval (int): number of seconds to wait between each run in continuous mode. Defaults to 30 seconds.
        
        Target table will be optimized following merge. If running in continuous mode, optimize will be performed every 8 merges. 
        """

        for i in itertools.count(start=1):
            if i % 8 == 0:
                self.optimize()
            
            start_time, end_time = self.get_time_filter()
            query_hist = self.get_query_history(start_time, end_time, include_metrics=True)
            query_hist_df = self.create_dataframe(query_hist)
            self.load_query_history(query_hist_df, start_time)
            
            if self.pipeline_mode == 'triggered':
                self.optimize()
                break
            
            time.sleep(trigger_interval)

def main():
    """Used as entry point to run module from the command line with arguments"""

    args = sys.argv[1:]
    if len(args) < 6:
      print("Usage: dbsql_query_logger.py catalog, schema, table, pipeline_mode, backfill_period, reset")
      sys.exit(1)
    
    catalog = sys.argv[1]
    schema = sys.argv[2]
    table = sys.argv[3]
    pipeline_mode = sys.argv[4]
    backfill_period = sys.argv[5]
    reset = sys.argv[6]

    query_logger = QueryLogger(
        catalog = catalog,
        schema = schema,
        table = table,
        pipeline_mode = pipeline_mode,
        backfill_period = backfill_period,
        reset = reset
    )

    query_logger.create_target_table()
    query_logger.run()

if __name__ == "__main__":
    main()