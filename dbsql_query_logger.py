import sys
import time
import logging
import itertools
from datetime import datetime, timezone
from typing import Iterator, Optional
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, MapType, BooleanType
import pyspark.sql.functions as F
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import QueryFilter, QueryInfo
from databricks.sdk.service.sql import TimeRange
from databricks.connect.session import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()
logger = logging.getLogger(__name__)

class QueryLogger:
    """Gets DBSQL query history from the Databricks API and merges it into a Delta Lake table.

    Attributes:
        catalog (str): Catalog name  
        schema (str): Schema name  
        table (str): Table name  
        start_time (Optional[datetime]): Limit results to queries that started after this time  
        end_time (Optional[datetime]): Limit results to queries that started before this time  
        user_ids (Optional[list[int]]): A list of user IDs who ran the queries  
        warehouse_ids (Optional[list[str]]): A list of warehouse IDs  
        include_metrics (bool): Whether to include metrics about query. Defaults to True.  
        pipeline_mode (str): If set to 'triggered', will load data and exit.  
            Otherwise will load data every 10 seconds. Defaults to 'triggered'.  
        backfill_period (str): Controls how far back to look for the initial load.  
            Defaults to '7 days'.  
        reset (str): If set to 'yes', the target table will be replaced. Defaults to 'no'.  
        additional_cols (dict): Dictionary of additional columns. Provide the column name  
            as the key, and a SQL expression for the value.  
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        table: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        user_ids: Optional[list[int]] = None,
        warehouse_ids: Optional[list[str]] = None,
        include_metrics: bool = True,
        pipeline_mode: str = 'triggered',
        backfill_period: str = '7 days',
        reset: str = 'no',
        additional_cols: Optional[dict] = None
    ):
        if catalog not in(None, ''):
            self.catalog = catalog
        else:
            raise Exception("A catalog is required but was not provided")
        if schema not in(None, ''):
            self.schema = schema
        else:
            raise Exception("A schema is required but was not provided")
        if table not in(None, ''):
            self.table = table
        else:
            raise Exception("A table is required but was not provided")
        self.schema = schema
        self.table = table
        self.start_time = start_time
        self.end_time = end_time
        self.user_ids = user_ids
        self.warehouse_ids = warehouse_ids
        self.include_metrics = include_metrics
        self.pipeline_mode = pipeline_mode
        self.backfill_period = backfill_period
        self.reset = reset
        self.additional_cols = additional_cols
        self.w = WorkspaceClient()

    def create_target_table(self) -> None:
        """Creates target Delta Lake table."""

        spark.sql(f'use catalog {self.catalog}')
        spark.sql(f'create schema if not exists {self.schema}')
    
        create_tbl_stmt = (
            "create or replace table" if self.reset == "yes" else "create table if not exists"
        )
    
        spark.sql(
            f"""
                {create_tbl_stmt} {self.catalog}.{self.schema}.{self.table} (
                  query_id STRING comment 'The query ID.',
                  status STRING comment 'Query status with one the following values:\n- `QUEUED`: Query has been received and queued.\n- `RUNNING`: Query has started.\n- `CANCELED`: Query has been cancelled by the user.\n- `FAILED`: Query has failed.\n- `FINISHED`: Query has completed.',
                  query_text STRING comment 'The text of the query.',
                  query_start_time TIMESTAMP comment 'The time the query started.',
                  execution_end_time TIMESTAMP comment 'The time execution of the query ended.',
                  query_end_time TIMESTAMP comment 'The time the query ended.',
                  user_id BIGINT comment 'The ID of the user who ran the query.',
                  user_name STRING comment 'The email address or username of the user who ran the query.',
                  spark_ui_url STRING comment 'URL to the query plan in the Spark UI. For advanced troubleshooting.',
                  warehouse_id STRING comment 'Warehouse ID.',
                  error_message STRING comment 'Message describing why the query could not complete.',
                  rows_produced BIGINT comment 'The number of results returned by the query.',
                  metrics MAP <STRING, STRING> comment 'Metrics about query execution. See the [API documentation](https://docs.databricks.com/api/workspace/queryhistory/list#res-metrics) for descriptions of available metrics.',
                  is_final BOOLEAN comment 'Whether more updates for the query are expected.',
                  channel_used MAP <STRING, STRING> comment 'Channel information for the SQL warehouse at the time of query execution.',
                  duration BIGINT comment 'Total execution time of the query from the client’s point of view, in milliseconds.',
                  executed_as_user_id BIGINT comment 'The ID of the user whose credentials were used to run the query.',
                  executed_as_user_name STRING comment 'The email address or username of the user whose credentials were used to run the query.',
                  plans_state STRING comment 'Whether plans exist for the execution, or the reason why they are missing. Potential values are `IGNORED_SMALL_DURATION | IGNORED_LARGE_PLANS_SIZE | EXISTS | UNKNOWN | EMPTY | IGNORED_SPARK_PLAN_TYPE`',
                  statement_type STRING comment 'Type of statement. Potential values are `OTHER | ALTER | ANALYZE | COPY | CREATE | DELETE | DESCRIBE | DROP | EXPLAIN | GRANT | INSERT | MERGE | OPTIMIZE | REFRESH | REPLACE | REVOKE | SELECT | SET | SHOW | TRUNCATE | UPDATE | USE`'
                )
                using delta cluster by (query_start_time)
                tblproperties (
                  'delta.enableDeletionVectors' = 'true'
                )
            """
        )
        
        logger.info(f"Created table {self.catalog}.{self.schema}.{self.table} if it did not already exist.")
    
    def _get_time_filter(self) -> None:
        """Gets time filters for query history API.
        
        To enable incremental loads, the starting time is obtained from the query_history table in the following order:
        1. min(query_start_time) of queries in 'QUEUED' or 'RUNNING' status within the past 3 days
        2. max(query_start_time)
        3. current_date() - backfill_period (Initial loads only)

        The end time is the current timestamp.
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
                    and query_start_time >= current_date() - interval 3 days
                ),
                max(query_start_time),
                current_date() - interval {self.backfill_period}
              )
            from
              {self.catalog}.{self.schema}.{self.table}
        """
        
        self.start_time = spark.sql(start_time_query).collect()[0][0]
        self.end_time = datetime.now(tz=timezone.utc)
        
        logger.info(f'API filter start time: {self.start_time.strftime("%Y-%m-%d %H:%M:%S")}, end time: {self.end_time.strftime("%Y-%m-%d %H:%M:%S")}')
    
    def get_query_history(self) -> Iterator[QueryInfo]:
        """Gets DBSQL query history using the Databricks Python SDK.

        https://docs.databricks.com/api/workspace/queryhistory/list
        
        Yields:
            Iterator[QueryInfo]: generator containing results from the DBSQL query history API
        """
        
        if self.start_time is None:
            raise RuntimeError('start_time variable cannot be None. It must be provided when creating a QueryLogger instance or set by _get_time_filter() when incremental_load=True in run().')
        if self.end_time is None:
            raise RuntimeError('end_time variable cannot be None. It must be provided when creating a QueryLogger instance or set by _get_time_filter() when incremental_load=True in run().')
        
        start_time_ms = int(self.start_time.timestamp() * 1000)
        end_time_ms = int(self.end_time.timestamp() * 1000)
        
        logger.info(f'Retrieving query history. This can take a while with larger data volumes.')

        query_hist_list = self.w.query_history.list(
            include_metrics=self.include_metrics,
            max_results=1000,
            filter_by = QueryFilter(
                user_ids=self.user_ids,
                warehouse_ids=self.warehouse_ids,
                query_start_time_range = TimeRange(
                    start_time_ms=start_time_ms, end_time_ms=end_time_ms
                )
            )
        )

        return query_hist_list
    
    def create_dataframe(self, query_hist_list) -> DataFrame:
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
        
        if self.additional_cols: 
            for k,v in self.additional_cols.items():
                query_hist_parsed_df = query_hist_parsed_df.withColumn(k, F.expr(f"{v}"))
        
        return query_hist_parsed_df
    
    def load_query_history(self, query_hist_parsed_df: DataFrame) -> None:
        """Merges data into a Delta Lake table. Schema evolution is enabled.
        
        The target table is filtered using query_start_time >= '{query_start_time}' to reduce the search space for matches and enable file skipping.
        This optimization is possible because the source data (With the exception of the initial load) will only contain updates for queries that were previously in progress.
        
        Args:
            query_hist_parsed_df (DataFrame): DataFrame containing DBSQL query history
        """

        spark.conf.set('spark.databricks.delta.schema.autoMerge.enabled', True)
        tgt_table = DeltaTable.forName(spark, f'{self.catalog}.{self.schema}.{self.table}')
        
        if self.start_time is None:
            raise RuntimeError('start_time variable cannot be None. It must be provided when creating a QueryLogger instance or set by _get_time_filter() when incremental_load=True in run().')
        query_start_time = self.start_time.strftime("%Y-%m-%d")

        logger.info(f"Merging data into {self.catalog}.{self.schema}.{self.table}. Target table filtered using query_start_time >= '{query_start_time}' to optimize the merge.")
        
        merge = (
            tgt_table.alias("t")
            .merge(query_hist_parsed_df.alias("s"), f"t.query_id = s.query_id and t.query_start_time >= '{query_start_time}'")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        logger.info(f"Merge completed")

    def optimize(self) -> None:
        """Optimize Delta Lake table.
        
        The target table uses Liquid Clustering by default. If ZORDER is used, update the optimize command below to include ZORDER columns.
        """

        spark.sql(f'optimize {self.catalog}.{self.schema}.{self.table}')
        logger.info(f'Optimized table {self.catalog}.{self.schema}.{self.table}')
    
    def run(self, filter_current_user: bool = False, incremental_load: bool = True, trigger_interval: int = 30) -> None:
        """Runs DBSQL query logger pipeline.

        Args:
            filter_current_user (bool): If set to true, only queries for the current user will be retrieved.  
                Defaults to false.  
            incremental_load (bool): If set to true, data retrieval will be incremental.  
                Defaults to true.  
            trigger_interval (int): number of seconds to wait between each run in continuous mode.  
                Defaults to 30 seconds.  
        
        Target table will be optimized following merge. If running in continuous mode, optimize will be performed every 8 merges. 
        """
        
        self.create_target_table()

        if filter_current_user:
            current_user = self.w.current_user.me()
            current_user_id = int(current_user.id or 0) # Default to 0 to avoid None
            if current_user_id == 0:
                raise RuntimeError('Failed to get current user')
            logger.info(f'Filtering to current user id {current_user.user_name}')
            self.user_ids = [current_user_id]
        
        for i in itertools.count(start=1):
            if i % 8 == 0:
                self.optimize()
            
            if incremental_load:
                self._get_time_filter()
            
            query_hist = self.get_query_history()
            query_hist_df = self.create_dataframe(query_hist)
            self.load_query_history(query_hist_df)
            
            if self.pipeline_mode == 'triggered':
                self.optimize()
                break
            
            time.sleep(trigger_interval)

def main() -> None:
    """Used as entry point to run module from the command line with arguments.
    
    The main function is intended for incremental bulk collection and doesn't support all arguments.
    Support for additional arguments can be added as needed.
    """

    logging.basicConfig(
        format="%(asctime)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z"
    )
    
    logger.setLevel(logging.INFO)

    args = sys.argv[1:]
    if len(args) < 6:
        print("Incorrect number of arguments provided")
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

    query_logger.run()

if __name__ == "__main__":
    main()