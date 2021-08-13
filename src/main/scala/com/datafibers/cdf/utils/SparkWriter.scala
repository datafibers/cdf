package com.datafibers.cdf.utils

import com.google.gson.JsonParser
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.{NullType, StringType}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.elasticsearch.spark.sql.sparkDatasetFunctions
import org.slf4j.LoggerFactory

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.Map
import scala.collection.convert.WrapAsScala

trait SparkWriter extends SetupFunc with SparkSQLFunc {
  val writerLogger = LoggerFactory.getLogger(this.getClass.getName)

  val badChaInColNamePattern = "\\s|\\?|\\(|\\)||\\-|\\$|\\@|\\[|\\]|\\]"

  def writeDFToParquet(processDf: DataFrame, filePath: String) = {
    processDf.columns.foldLeft(processDf)((r, col) =>
      r.withColumnRenamed(col, col.replaceAll(AppDefaultConfig.BAD_CHAR_IN_COL_NAME_PATTERN, "__"))
    ).write.mode(SaveMode.Overwrite).parquet(filePath)
  }

  def writeDFToParquetInPar(processDf: DataFrame, filePath: String, partitionColName: String) = {
    processDf.columns.foldLeft(processDf)((r, col) =>
      r.withColumnRenamed(col, col.replaceAll(AppDefaultConfig.BAD_CHAR_IN_COL_NAME_PATTERN, "__"))
    ).write.mode(SaveMode.Overwrite).partitionBy(partitionColName).parquet(filePath)
  }

  def writeDFToElastic(processDf: DataFrame, index: String, inputConfig: Map[String, Any]) = {
    val indexTimeStamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
    val createIndex = index.split("@distributeBy_")(0).replaceAll("@timestamp", s"_${indexTimeStamp}")
    val prefixIndex = index.split("@distributeBy_")(0).replaceAll("@timestamp", "")
    val deleteIndex = prefixIndex + "_2*" // wildcard to delete index and alias starting with timestamp
    val hasEsMappingId = processDf.columns.contains("etl_es_id")
    val hasEsBatchId = processDf.columns.contains("etl_batch_id")
    val body = schemaToMapping(inputConfig, processDf).toString
    val sparkConf = processDf.sqlContext.sparkSession.conf
    val host = sparkConf.get("es.nodes").split(",")(0)
    val port = sparkConf.get("es.port").toInt
    val user = sparkConf.get("es.net.http.auth.user")
    val pass = sparkConf.get("es.net.http.auth.pass")
    val retryCnt = sparkConf.get("es.batch.write.retry.count").toInt
    val retrySec = sparkConf.get("es.batch.retry.wait").replaceAll("s", "").toInt // 60s => 60
    val rowComRetryCnt = sparkConf.get("es.rowcnt.compare.retry.count").toInt
    val rowComRetrySec = sparkConf.get("es.rowcnt.compare.retry.wait").replaceAll("s", "").toInt // 60s => 60

    writerLogger.info(s"Elastic call: schemaToMapping = ${body}")
    if (inputConfig.getOrElse("idx_create", "false").toString == "true")
      writerLogger.info(s"Elastic call: create idx with schema having response = " + curlClient("PUT", host, port, createIndex, body, user, pass))
    if (inputConfig.getOrElse("idx_purge_on_time", "false").toString == "true")
      writerLogger.info(s"Elastic call: delete old idx with response = " + curlClient("DELETE", host, port, s"${deleteIndex},-${createIndex}", body, user, pass))
    if (inputConfig.getOrElse("idx_alias_create", "false").toString == "true") {
      val alias = inputConfig.getOrElse("idx_alias", prefixIndex).toString
      val bodyDelete = "{\"actions\":[{\"add\":{\"index\":\"" + createIndex + "\",\"alias\":\"" + alias + "\"}}]}"
      writerLogger.info(s"Elastic call: created alias = ${alias} for index = ${createIndex} with response = " + curlClient("POST", host, port, "_aliases", body, user, pass))
    }

    /*
     convert all dataframes type to string before sending to elastic.
     For example, dataframe.date to elastic.date does not work for for current version of elastic 7
     */
    val allStrDf = processDf.columns.foldLeft(processDf)((r, c) => r.withColumn(c, processDf.col(c).cast("String")))
    writerLogger.info(s"Elastic call: convert dataframe type to string with schema = " + allStrDf.schema.toString)

    (hasEsBatchId) match {
      case (false) => writeToEsWithRetry(allStrDf, createIndex, hasEsMappingId, retryCnt, retrySec)
      case (true) => {
        val partitions = allStrDf.select("etl_batch_id").distinct.collect.flatMap(_.toSeq).map(_.toString)
        partitions.foreach(par => {
          writeToEsWithRetry(allStrDf.where(s"etl_batch_id == '${par}'").drop("etl_batch_id"), createIndex, hasEsMappingId, retryCnt, retrySec)
          writerLogger.info(s"partition etl_batch_id = '${par}' is saved to index ${createIndex}")
        })
      }
    }
    // when es.mapping.id or etl_es_id is provided, the rwo count check is performed in case the etl_es_id is not unique in source data
    if (hasEsMappingId) checkEsRowCountWithRetry(allStrDf.count, createIndex, host, port, user, pass, rowComRetryCnt, rowComRetrySec)
  }

  def checkEsRowCountWithRetry(esExpectedCount: Long, index: String, host: String, port: Int, user: String, pass: String, failureRetryMaxCount: Int, failureRetryWaitSeconds: Int) = {
    var k = 0
    while (k >= 0) {
      // refresh the newly created index
      curlClient("GET", host, port, index + "/_refresh", "", user, pass)
      val rowCountResponse = curlClient("GET", host, port, index + "/_count", "", user, pass).getOrElse("cmd_output", "").toString
      val esActualCount = new JsonParser().parse(rowCountResponse).getAsJsonObject.get("count").getAsLong
      if (esExpectedCount == esActualCount) {
        writerLogger.info(s"esExpectedCount:${esExpectedCount} = esActualCount:${esActualCount}")
        k = -1 // exit loop when row count are matched
      } else {
        writerLogger.error(s"Row count comparing for ${index} is failed (esExpectedCount:${esExpectedCount} <> esActualCount:${esActualCount}) at retry ${k}/${failureRetryMaxCount} and continue retry after ${failureRetryWaitSeconds}")
        Thread.sleep(failureRetryWaitSeconds * 1000)
        if (k >= failureRetryMaxCount) {
          writerLogger.error(s"Row count comparing for ${index} is failed after ${failureRetryMaxCount} retry")
          throw new Exception(s"esExpectedCount:${esExpectedCount} <> esActualCount:${esActualCount} finally timeout")
        }
        k += 1
      }
    }
  }

  def writeToEsWithRetry(processDf: DataFrame, index: String, indexMapFlag: Boolean, failureRetryMaxCount: Int, failureRetryWaitSeconds: Int) = {
    var k = 0
    while (k >= 0) {
      try {
        if (!indexMapFlag) processDf.saveToEs(index) else processDf.saveToEs(index, Map("es.mapping.id" -> "etl_es_id"))
        writerLogger.info(s"Calling saveToEs(${index}) is completed.")
        k = -1 // exit loop when data is saved to elastic
      } catch {
        case e: Throwable => {
          if (k >= failureRetryMaxCount) {
            writerLogger.error(s"Calling saveToEs(${index}) is failed with exception ${e.toString}.")
            throw new Exception(e.toString)
          } else {
            k += 1
            writerLogger.error(s"Calling saveToEs(${index}) is failed at retry ${k}/${failureRetryMaxCount} and continue retry after ${failureRetryWaitSeconds}")
            Thread.sleep(failureRetryWaitSeconds * 1000)
          }
        }
      }
    }
  }

  def writeDFToCSV(processDf: DataFrame, filePath: String, header: Boolean = true) = {
    processDf.write.mode(SaveMode.Overwrite)
      .format("csv").option("delimiter", "|").option("header", header).save(filePath)
  }

  def writeOutputToCSV(processDf: DataFrame, filePath: String, fileProperties: Map[String, String] = AppDefaultConfig.DEFAULT_FILE_OUTPUT_PROS) = {
    val expres = processDf.schema.fields.map { f =>
      val dataType = f.dataType
      dataType match {
        case NullType => col(f.name).cast(StringType).alias(f.name) // cast null to spark sql supported type
        case _ => regexp_replace(col(f.name), "\\|", "?").cast(f.dataType).alias(f.name)
      }
    }
    val header = fileProperties.getOrElse("header", AppDefaultConfig.DEFAULT_FILE_OUTPUT_PROS("header"))
    val delimiter = fileProperties.getOrElse("delimiter", AppDefaultConfig.DEFAULT_FILE_OUTPUT_PROS("delimiter"))
    val quote = fileProperties.getOrElse("delimiter", AppDefaultConfig.DEFAULT_FILE_OUTPUT_PROS("quote"))

    processDf.select(expres: _*).write.mode(SaveMode.Overwrite).format("csv")
      .option("quote", quote)
      .option("delimiter", delimiter)
      .option("header", header)
      .option("ignoreLeadingWhiteSpace", fileProperties.getOrElse("ignoreLeadingWhiteSpace", AppDefaultConfig.DEFAULT_FILE_OUTPUT_PROS("ignoreLeadingWhiteSpace")))
      .option("ignoreTrailingWhiteSpace", fileProperties.getOrElse("ignoreTrailingWhiteSpace", AppDefaultConfig.DEFAULT_FILE_OUTPUT_PROS("ignoreTrailingWhiteSpace")))
      .save(filePath)
  }

  def writeDFToCSVInPar(processDf: DataFrame, filePath: String, partitionColName: String) = {
    processDf.columns.foldLeft(processDf)((r, col) =>
      r.withColumnRenamed(col, col.replaceAll(AppDefaultConfig.BAD_CHAR_IN_COL_NAME_PATTERN, "__"))
    ).write.mode(SaveMode.Overwrite).format("csv")
      .option("delimiter", "|")
      .option("header", "true")
      .partitionBy(partitionColName).save(filePath)
  }

  def appendDFToCSV(processDf: DataFrame, filePath: String, header: Boolean = true) = {
    processDf.write.mode(SaveMode.Append).format("csv")
      .option("delimiter", "|").option("header", "true").save(filePath)
  }

  def writeDFToJSON(processDf: DataFrame, filePath: String) = {
    processDf.write.mode(SaveMode.Overwrite).format("json").save(filePath)
  }

  def appendDFToJSON(processDf: DataFrame, filePath: String) = {
    processDf.write.mode(SaveMode.Append).format("json").save(filePath)
  }

  def writeDFToHive(processDf: DataFrame, tableName: String) = {
    processDf.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
  }

  def writeDFToFile(fileType: String, processDf: DataFrame, filePath: String, writeMode: String, parColName: String) = {
    if (fileType.equalsIgnoreCase("parquet")) {
      if (writeMode.equalsIgnoreCase("all")) writeDFToParquet(processDf, filePath + s"/run_date=${parColName}")
      else writeDFToParquetInPar(processDf, fileType, parColName)
    } else if (fileType.equalsIgnoreCase("json")) {
      writeDFToJSON(processDf, filePath)
    } else {
      if (writeMode.equalsIgnoreCase("all")) writeDFToCSV(processDf, filePath + s"/run_date=${parColName}")
      else writeDFToCSVInPar(processDf, filePath, parColName)
    }
  }

  def writeDFToOutput(inputConfig: Map[String, Any], processDf: DataFrame, parColName: String) = {
    val fileType = inputConfig.getOrElse("output_type", "").toString
    val writeMode = if (inputConfig.contains("output_partition")) "last" else "all"
    val filePath = inputConfig("output").toString

    if (fileType.equalsIgnoreCase("parquet")) {
      if (writeMode.equalsIgnoreCase("all")) writeDFToParquet(processDf, filePath + s"/run_date=${parColName}")
      else writeDFToParquetInPar(processDf, fileType, parColName)
    } else if (fileType.equalsIgnoreCase("json")) {
      writeDFToJSON(processDf, filePath)
    } else if (fileType.equalsIgnoreCase("elastic")) {
      writeDFToElastic(processDf, inputConfig.getOrElse("idx_name", inputConfig("app_code").toString).toString, inputConfig)
    } else if (fileType.equalsIgnoreCase("hive")) {
      writeDFToHive(processDf, inputConfig("hive_db_tbl_name").toString)
    } else {
      if (writeMode.equalsIgnoreCase("all")) writeDFToCSV(processDf, filePath + s"/run_date=${parColName}")
      else writeDFToCSVInPar(processDf, filePath, parColName)
    }
  }

  def writeDFToOutputs(inputConfig: Map[String, Any], processDf: DataFrame, lastProcessedDate: String) = {
    if (inputConfig.contains("outputs")) {
      val inputSource = inputConfig.get("outputs").get.asInstanceOf[java.util.List[java.util.Map[String, Any]]]
      val scalaList = WrapAsScala.asScalaBuffer(inputSource).toList

      scalaList.map({ case x: java.util.Map[String, Any] =>
        val config = WrapAsScala.mapAsScalaMap(x).asInstanceOf[Map[String, String]]
        val disabled = config.getOrElse("disabled", AppDefaultConfig.DEFAULT_CONF_DISABLE).toBoolean
        if (!disabled) writeDFToOutput(config, processDf, config.getOrElse("output_partition", lastProcessedDate).toString)
      })
    } else writeDFToOutput(inputConfig, processDf, inputConfig.getOrElse("output_partition", lastProcessedDate).toString)
  }

}
