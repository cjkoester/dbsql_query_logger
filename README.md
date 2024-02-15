# Databricks SQL Query Logger

Retrieves Databricks SQL (DBSQL) [query history](https://docs.databricks.com/api/workspace/queryhistory/list) and merges it into a Delta Lake table.

A [System Table](https://docs.databricks.com/en/administration-guide/system-tables/index.html) containing similar data is on the Databricks roadmap, so solutions like this will be mostly unecessary in the near future.

## Getting Started

### Dependencies

* This project was developed using the [Databricks SDK](https://docs.databricks.com/en/dev-tools/sdk-python.html) 0.18.0. The Databricks SDK is in beta at the time of writing, and can be used in production. Be sure to test other versions prior to use.
* The target Delta Lake table uses [Deletion Vectors](https://docs.databricks.com/en/delta/deletion-vectors.html) (DBR 14.1 and above) and [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html) (DBR 13.3 and above). If you need to support an earlier DBR, update the create_target_table function accordingly.

### Usage
The DBSQL query logger can be used in a variety of ways.

* Add the dbsql_query_logger.py module as a [workspace file](https://docs.databricks.com/en/files/workspace-modules.html).
* Use a whl to install it on a cluster.
* A Databricks Workflow can run it using the module or whl.
