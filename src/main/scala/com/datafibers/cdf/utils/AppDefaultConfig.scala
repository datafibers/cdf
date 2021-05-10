package com.datafibers.cdf.utils

import scala.collection.immutable.HashMap

object AppDefaultConfig {
  // default for spark aps
  val DEFAULT_APP_SPARK_MASTER = "yarn-cluster"
  val DEFAULT_APP_DATE = "1900-01-01"

  // pattern to replace from column name so that spark sql support such col names
  val BAD_CHAR_IN_COL_NAME_PATTERN = "\\s|\\?|\\(|\\)||\\-|\\$|\\@|\\[|\\]|\\]"

  // default for help function
  lazy val DEFAULT_UDF_CURRENCIES: List[String] = List("USD", "CAD", "CHF", "ERU", "JPY", "RMB")
  val DEFAULT_UDF_DATE = "1800-01-01"
  val DEFAULT_UDF_FLOAT = "0.0"
  val DEFAULT_UDF_STRING = "null"
  val DEFAULT_DATE_PATTERN = "yyyy-MM-dd"

  // default for data catchup
  val DEFAULT_CATCHUP_MIN_FILE_SIZE = 12
  val DEFAULT_CATCHUP_CHECK_DATE = "2000-01-01"

  // default for hive
  val DEFAULT_HIVE_PATH = "/apps/hive/warehouse/"

  // default for file source
  val DEFAULT_FILE_INPUT_PROS =
    Map("header" -> "true", "delimiter" -> "|", "inferSchema" -> "false", "ignoreLeadingWhiteSpace" -> "true",
      "ignoreTrailingWhiteSpace" -> "true", "hasExtraHeader" -> "false", "extraHeaderRowCount" -> "false",
      "headerTrailerInclusiveFooter" -> "false", "header_delimiter" -> "|", "quote" -> "")

  val DEFAULT_FILE_OUTPUT_PROS =
    Map("header" -> "true", "delimiter" -> "|", "quote" -> "",
      "ignoreLeadingWhiteSpace" -> "true", "ignoreTrailingWhiteSpace" -> "true")

  // default for config properties
  val DEFAULT_CONF_DB_NAME = "default"
  val DEFAULT_CONF_VALUE = ""
  val DEFAULT_CONF_DISABLE = "false"
  val DEFAULT_CONF_LOOK_BACK = 12
  val DEFAULT_CONF_READ_STRATEGY = "all"
  val DEFAULT_CONF_TABLE_TYPE = "reference"
  val DEFAULT_CONF_SQL_RESOURCE_ROOT = "/sql"
  val DEFAULT_CONF_FILE_PATH = ""
  val DEFAULT_CONF_ES_IDX_NAME = "test"
  val DEFAULT_CONF_ES_TYPE_NAME = "metric"

  val DEFAULT_CONF_STAGE_OUTPUT_ROOT_PATH = "tmp/staging"
  val DEFAULT_CONF_STAGE_LOG_ROOT_PATH = "tmp/staging/log"
  val DEFAULT_CONF_TREE_OUTPUT = "tmp/tree"

  // global init sql
  val GLOBAL_INIT_SQL =
    """
      |set gsql_msg_start = "GLOBAL SQL STARTED";
      |set test = 1;
      |set gsql_msg_end = "GLOBAL SQL ENDED";
      |""".stripMargin

  val DF_DUMMY_RESPONSE =
    """
      |{"runId":"2022-01-01"}
      |""".stripMargin

  val OPTIONAL_FIELDS_MAP = HashMap(
    "default" -> "",
    "gwes_cia" -> "new_col0,new_col1"
  )

  val OPTIONAL_FIELDS_REPLACE = "#NA#"
}
