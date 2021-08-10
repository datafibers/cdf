package com.datafibers.cdf.utils

import org.apache.spark.sql.SparkSession

import java.nio.file.FileSystemException
import scala.collection.Map
import scala.collection.convert.WrapAsScala

trait SourceToDS extends SparkSQLFunc with SparkReader with HelpFunc {

  def sourceToDS(inputConfig: Map[String, Any])(implicit spark: SparkSession): String = {
    val sqlFilePath = inputConfig.getOrElse("sql_resource_root",
      AppDefaultConfig.DEFAULT_CONF_SQL_RESOURCE_ROOT).toString + "/sql_" + inputConfig("app_code") + ".sql"

    if (inputConfig.contains("sources")) {
      val inputSources = inputConfig.get("sources").get.asInstanceOf[java.util.List[java.util.Map[String, Any]]]
      val scalaList = WrapAsScala.asScalaBuffer(inputSources).toList

      headList(scalaList.map({
        case x: java.util.Map[String, Any] =>
          val config = WrapAsScala.mapAsScalaMap(x).asInstanceOf[Map[String, String]]
          val disabled = config.getOrElse("disabled", AppDefaultConfig.DEFAULT_CONF_DISABLE).toBoolean
          val tableType = config.getOrElse("type", AppDefaultConfig.DEFAULT_CONF_TABLE_TYPE)

          if (!disabled) {
            if (tableType.contains("file")) { // this is for file processing
              try {
                readFromFile(config)
              } catch {
                case e: Throwable =>
                  throw new FileSystemException(
                    "Reading source failed using app_" + inputConfig("app_code") + ".ynl with path: " +
                      config.getOrElse("path", "path are not defined in yml") + " and format: " +
                      config.getOrElse("format", "csv"),
                    e.getMessage + "\n" + e.getCause, e.toString
                  )
              }
              applyTableRowFilter(config)
              findLastProcessFromFile(config)
            } else if (tableType.contains("elastic")) {
              readFromElastic(config)
              applyTableRowFilter(config)
              findLastProcessFromFile(config)
            } else if (tableType.contains("jdbc")) {
              readFromJDBC(config)
              applyTableRowFilter(config)
              findLastProcessFromFile(config)
            } else {
              readFromHive(config)
              applyTableRowFilter(config)
              findLastProcessFromFile(config)
            }
          } else null
      }).filter(_ != null))
    } else findLastProcessFromSQL(sqlFilePath)
  }
}
