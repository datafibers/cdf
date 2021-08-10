<p align="left"><img src="https://cryptologos.cc/logos/binance-coin-bnb-logo.png" width="150">   <img src="https://miro.medium.com/max/9074/1*RoFb2sFULMV-gnOy727FoQ.png" width="400"></p>

Connective Data Framework (CDF) is designed to be a generic data processing framework to read various of data source, such as hive, flat files, database, elastic search, Kafka, etc), perform across data source transformation, and write to the different destinations. CDF is implemented using Scala and Apache Spark.

# Data Source Specific Files
Follow files are required to be customized for each data source. Each pipeline, which reads, transform, and write data, is called a application. Here, we use ```APP_CODE``` to identify each application.

| File Name | Functions|
|-----------|----------|
|`src/main/resources/conf/app_{APP_CODE}.yml`| data source specific configuration file|
|`src/main/resources/sql/app_{APP_CODE}.sql`| data source specific configuration file|
|`src/test/resources/setup/tsql_${APP_CODE}.sql`| data source specific test data preparation file|

# Application Configurations
All application settings are configured through YML files in the ```src/main/resources/conf/application.yml.{env}```. They are different sessions for Spark, Hive, Elastic settings applied to the whole project.

# Data Source/Target Configurations
In the following sessions, we'll introduce how to configure the source/target config files in ```src/main/resources/conf/app_{APP_CODE}.yml```.
## Generic Source Configuration Keys
Following yml keys are supported for reading different data sources categories. These keys are defined under `source` key in the yml files.

|Key            |Category|Request  |Default| Functions|
|-----------|----------|----------|-----------|----------|
|`alias`|all|optional|table|alias for the data source|
|`type`|all|optional|reference|table type, driver or reference|
|`row_filter`|all|optional||row number filter with partition by and order by fields|
|`read_strategy`|all|optional|all|how to read data, all -  all data. latest - the latest partition.|
|`disabled`|all|optional|false|disable this source's config|
|`database`|hive/database|optional|default|hive database name. Can also be specified with table name, such as, database_name.table_name|
|`table`|hive/database|mandatory||hive table name.|
|`regx`|hive|optional| |database name with regular expression, such as test_prod_([0-9]{8}).|
|`look_back`|hive|optional|12|number of database to read according to `regx`|
|`path`|file|mandatory| |the path where to read the file. `*` is supported to match all.|
|`delimitor`|file|optional| `\|`|file fields delimitor |
|`quote`|file|optional|`""`|file quote|
|`optional_fields`|file|optional||list of comma seperated columns when they are not avaliable in source giving default value as placeholder |

## Generic Target Configuration Keys
Following yml keys are supported for writing different data targets.

|Key            |Category|Request  |Default| Functions|
|-----------|----------|----------|-----------|----------|
|`disabled`|all|optional|false|disable this target's config|
|`outputs`|all|optional| |when it is specified, you can define multiple output under this key|
|`output`|all|implicit| |output file root path in HDFS/ELASTIC. By default it gets value from `${DE_OUTPUT_ROOT_PATH}`|
|`output_log`|all|implicit| |output log root path in HDFS. By default it gets value from `${DE_LOG_ROOT_PATH}`|
|`output_type`|all|optional|csv|output type supported, such as csv, paruqet, avro, json, hive, elastic.|
|`hive_db_tbl_name`|hive|optional| |when `output_type` is hive, use `database_name.table_name`.|
|`idx_name`|elastic|optional| app_code|when `output_type` is elastic, this is index name.|
|`@timestamp`|elastic|optional| |used in index name as metadata to append current timestamp to the index name to make it unique.|
|`idx_create`|elastic|optional|false |whether to create the index before writing data.|
|`idx_purge_on_time`|elastic|optional|false |when index is created with `@timestamp`, old indices are removed when it is `true`.|
|`idx_alias_create`|elastic|optional|false |whether to create alias|
|`idx_alias`|elastic|optional|idx_name|alias of the index, by default it is index name without timestamp|
|`idx_setting`|elastic|optional|  |whether to add setting section during index creation|
|`idx_map_rule`|elastic|optional| |type - generate mapping from dataframe data type. name - generate mapping from column postfix, such as address--idxm-keyword|
|`idx_type_else`|elastic|optional|text:dummy|this is requested when `idx_create` is `true`. For example, text:col1,col2, this is to set column with string type for col1 and col2 as text. For other string type columns, set it as keyword.|
|`idx_prop_ov`|elastic|optional|  |this is where to pass whole json string as mapping properties to overwrite auto generated index mappings|

## Application Configuration Keys
Key            |Category|Request  |Default| Functions|
|-----------|----------|----------|-----------|----------|
|`job_disabled`|apps|optional|false|disable this application code job by skipping it.|
|`dry_run`|apps|optional|false|verify the job without running the job.|
|`cob_discover`|apps|optional|false|auto discover the cob in yyyy-MM-dd from source read.|
|`lookback_defalt`|hive|optional| |default look back value for all source data.|
|`sql_resource_root`|sql|optional|../sql|default location where to read the sql files|
|`sql_file_part`|sql|apps|optional||specify how many sql partial files to read in bulk|
|`sql_init`|sql|optional|false|true, load `init.sql` ahead running all other sql files.|
|`sql_init_global`|sql|optional|false|true, load `AppDefaultConfig.GLOBAL_INIT_SQL` ahead running all other sql files.|
|`sql_casesensitive`|sql|optional|false|same to Spark sql case sensitive setting|
|`app_name`|apps|implicit||application name|
|`app_code`|apps|implicit||application code|

**Note:**
1. when `output_type` is elastic, the `etl_es_id` in the dataframe maps to `es.mapping.id`, which is the key of index.
1. when `output_type` is elastic, the `etl_batch_id` defines how many batch to divide and load data to elastic in sequence.

## Transformation Configuration Keys
CDF provides some internal transformations. However, it is suggested not to use them because of overhead. The suggested way
is to use transformations in the spark sql logic.

|Key            |Request  |Default| Functions|
|-----------|----------|-----------|----------|
|`cleaned_null_as`|optional|"null"|which value to represent null|
|`cleaned_date_fields`|optional| |format a list of comma separated columns as yyyy-MM-dd|
|`cleaned_amount_fields`|optional| |format a list of comma separated columns as proper amount value|
|`cetl_meta_enabled`|optional|false|append etl_pkey, etl_app_code, etl_start_time, and etl_end_time to data.|

# Environment Variable Files
CDF includes two environment variable files.
* `env.properties`: It keeps application code level properties for all application under management.
* `application.yml`: It keeps resource level properties, such as spark, hive, elastic settings.

**Note:**
The environment variable files may have `.prd` or `dev` extensions for production and non-production purpose.

When CDP launches the pipeline job, it looks for the `env.properties` in following orders

|Order            |Actions  |Comments|
|-----------|----------|-----------|
|1|search service account's own dir at `/home/${CURRENT_USER}/env.properties`|if `env.properties` may change for each run, you can generate it ahead.|
|2|search application's own dir at `./conf/env.properties`|if `env.properties` may not change, use this approach.|
|3|search global env settings at `/hadoop/data-products/env/env.properties`|if `env.properties` may not change and generic, use this approach.|

The list of environment variable supported in `env.properties` are as follows

|Key            |Request  |Default| Functions|
|-----------|----------|-----------|----------|
|`DF_BATCH_LATEST_URL`|optional||service url where to fetch the latest run id/date|
|`PA_BATCH_LATEST_URL`|optional||service url where to fetch the latest run id/date|
|`FETCH_SRC_LOC`|optional|false|if true, fetch source location root as `${DE_TREE_OUTPUT}` in format of `sys-${asset}`|
|`FETCH_RUN_ID`|optional|false|if ture, fetch run_id/date from service url and replace the one got from commandline|
|`DE_OUTPUT_ROOT_PATH`|optional|global-tree-entity/staging|hdfs location where to keep output file|
|`DE_LOG_ROOT_PATH`|optional|${DE_OUTPUT_ROOT_PATH}/log|hdfs location where to keep job log|
|`DE_YARN_QUEUE`|optional| |hadoop yarn queue|

# Run the Application
CDF can run the job in local or cluster mode by following setting.
```shell
export CONFIG_SPARK_MASTER=local
export CONFIG_SPARK_MASTER=yarn-cluster
```
To run the job, there are three options as follows.
```shell
./cdf_processing.sh ${app_code}
./cdf_processing.sh ${app_code} ${cob/closed_business_date}
./cdf_processing.sh ${app_code} ${cob/closed_business_date} ${spark_executor} ${spark_cores} ${spark_executor_memory} ${spark_driver_memory}
```
