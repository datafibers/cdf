<p align="left"><img src="https://cryptologos.cc/logos/binance-coin-bnb-logo.png" width="150">   <img src="https://miro.medium.com/max/9074/1*RoFb2sFULMV-gnOy727FoQ.png" width="400"></p>

COSP Data Framework (CDF) is designed to be a generic data processing framework to read various of data source, such as hive, flat files, database, elastic search, Kafka, etc), perform across data source transformation, and write to the different destinations. CDP is implemented using Scala and Apache Spark.

# Data Source Specific Files
Follow files are required to be cusomized for each data source. Each pipeline, which reads, transform, and write data, is called a application. Here, we use ```APP_CODE``` to identify each application.
| File Name | Functions|
|-----------|----------|
|`src/main/resources/conf/app_{APP_CODE}.yml`| data source specific configuration file|
|`src/main/resources/sql/app_{APP_CODE}.sql`| data source specific configuration file|
|`src/test/resources/setup/tsql_${APP_CODE}.sql`| data source specific test data preparision file|

# Appliction Configurations
All application settings are configured through YML files in the ```src/main/resources/conf/application.yml.{env}```. They are different sessions for Spark, Hive, Elastic settings applied to the whole project.

# Data Source/Target Configurations
In the following sessions, we'll introduce how to configurate the source/target config files in ```src/main/resources/conf/app_{APP_CODE}.yml```.
## Generic Source Configuration Keys
Following yml keys are supported for reading different data sources.
|Key            |Request  |Default| Functions|
|-----------|----------|-----------|----------|
|`database`|optional|default|hive database name. Can also be specified with table name, such as, database_name.table_name|
|`regx`|optional| |database name with regular expression, such as test_prod_([0-9]{8}).|
|`table`|mandatory||hive table name.
|`alias`|optional|table|alias for the data source
|`type`|optional|reference|table type, driver or referenec.
|`row_filter`|optional||row number filter with partition by and order by fields|
|`read_strategy`|optional|all|how to read data, all -  all data. latest - the latest partition.|
