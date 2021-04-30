![](doc/data-framework-logo.png)

COSP Data Framework (CDF) is designed to be a generic data processing framework to read various of data source, such as hive, flat files, database, elastic search, Kafka, etc), perform across data source transformation, and write to the different destinations. CDP is implemented using Scala and Apache Spark.

# Data Source Specific Files
Follow files are required to be cusomized for each data source. Each pipeline, which reads, transform, and write data, is called a application. Here, we use APP_CODE to identify each application.
| File Name | Functions|
|-----------|----------|
|src/main/resources/conf/app_{APP_CODE}.yml| data source specific configuration file|
|src/main/resources/sql/app_{APP_CODE}.sql| data source specific configuration file|
|src/test/resources/setup/tsql_${APP_CODE}.sql| data source specific test data preparision file|

# Appliction Configurations
