package com.datafibers.cdf.process

import com.datafibers.cdf.core.{IProcess, ProcessContext}
import com.datafibers.cdf.utils.{AppDefaultConfig, SourceToDS, SparkWriter}

class TransformationStep extends Serializable with SourceToDS with SparkWriter with IProcess {

  def process(context: ProcessContext) = {
    implicit val sparkSession = context.sparkSession
    val inputConfig = context.inputConfig
    val sqlFilePath =
      inputConfig.getOrElse("sql_resource_root", AppDefaultConfig.DEFAULT_CONF_SQL_RESOURCE_ROOT).toString +
      "/sql_" + inputConfig("app_code") + ".sql"

    System.setProperty("ppd", getMetricsPreviousProcessedDate(inputConfig("app_code").toString))

    // loop all tables config and register all tables as temp tables in the current schema
    val lastProcessedDateFromDriverTable = sourceToDS(inputConfig)
    val lastProcessedDate =
      if (lastProcessedDateFromDriverTable == null || lastProcessedDateFromDriverTable.isEmpty) System.getProperty("cob")
      else lastProcessedDateFromDriverTable

    // run the spark sql on registered tables
    val result = sqlRunner(inputConfig, sqlFilePath)
    val rowCnt = result.count

    // apply generic internal transformations
    val cleanedRes = etlMetaAppender(inputConfig, genericNullCleaner(inputConfig, genericAmountCleaner(inputConfig, result)))

    // when output specified partition, we only create last tun date file. Or else, keep all run dates.
    if (inputConfig.getOrElse("dry_run", "false") == "false") {
      val tarZeroRow = inputConfig.getOrElse("load_zero_row", "ignore").toString

      if(rowCnt == 0 && tarZeroRow == "exception")
        throw new RuntimeException("load_zero_row: exception setting throw exception when loading zero rows")
      else if (rowCnt == 0 && tarZeroRow == "ignore")
        appLogger.warn("[CDF] ignore loading zero rows")
      else
        writeDFToOutputs(inputConfig, cleanedRes, lastProcessedDate)

      context.outputInfo += ("last_processed_date" -> lastProcessedDate, "row_processed" -> rowCnt.toString)
    }
  }
}
