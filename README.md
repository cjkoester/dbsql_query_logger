# Databricks SQL Query Logger

Retrieves Databricks SQL query history using the Databricks SDK and merges it into a Delta Lake table.

## Getting Started

### Dependencies

* This solution was developed using version 0.18.0 of the Databricks SDK. The Databricks SDK is in beta at the time of writing, so be sure to test other versions prior to use.
* The target Delta Lake table uses [Deletion Vectors](https://docs.databricks.com/en/delta/deletion-vectors.html) (DBR 14.1 and above) and [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html) (DBR 13.3 and above). If you need to support an earlier DBR, update the create_target_table function accordingly.

### Installing
The DBSQL query logger can be installed and used in a variety of ways.

* The repository can be cloned in Databricks Repos.
* The dbsql_query_logger.py module can be added as a workspace file.

### Executing program

* How to run the program
* Step-by-step bullets
```
code blocks for commands
```

## Help

Any advise for common problems or issues.
```
command to run if program contains helper info
```

## Authors

Contributors names and contact info

ex. Dominique Pizzie  
ex. [@DomPizzie](https://twitter.com/dompizzie)

## Version History

* 0.2
    * Various bug fixes and optimizations
    * See [commit change]() or See [release history]()
* 0.1
    * Initial Release

## License

This project is licensed under the [NAME HERE] License - see the LICENSE.md file for details

## Acknowledgments

Inspiration, code snippets, etc.
* [awesome-readme](https://github.com/matiassingers/awesome-readme)
* [PurpleBooth](https://gist.github.com/PurpleBooth/109311bb0361f32d87a2)
* [dbader](https://github.com/dbader/readme-template)
* [zenorocha](https://gist.github.com/zenorocha/4526327)
* [fvcproductions](https://gist.github.com/fvcproductions/1bfc2d4aecb01a834b46)