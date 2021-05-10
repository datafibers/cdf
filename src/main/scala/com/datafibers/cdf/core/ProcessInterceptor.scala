package com.datafibers.cdf.core

import com.datafibers.cdf.model.SourceJobMetric
import com.datafibers.cdf.utils.{AppDefaultConfig, HelpFunc, SetupFunc, SparkWriter}
import org.slf4j.LoggerFactory

trait ProcessInterceptor extends IProcess with HelpFunc with SparkWriter {

  abstract override def process(context: ProcessContext) = {
    val processLogger = LoggerFactory.getLogger(this.getClass.getName)

    def getAppType(appName: String): String = {
      if (context.inputConfig.keySet.contains("app_type")) {
        context.inputConfig("app_type").toString.toLowerCase
      } else {
        if (appName.toLowerCase.contains("post")) "post"
        else if (appName.toLowerCase.contains("pre")) "pre"
        else "others"
      }
    }

    println("####ProcessInterceptor Entering####")

    val start = System.currentTimeMillis()
    var processStatus = "successful"
    try {
      super.process(context)
    } catch {
      case e: Exception => {
        processStatus = "failed"
        throw e
      }
    } finally {
      println("####ProcessInterceptor Exit####")

      try {
        val ppd = context.outputInfo.getOrElse("last_processed_date", context.outputInfo.get("cob").getOrElse(AppDefaultConfig.DEFAULT_APP_DATE))
        val lastProcessDate =
          if (ppd.contains("_"))
            dateScrubber(ppd.split("_")(0)) + "_" + ppd.split("_")(1)
          else dateScrubber(ppd)
        val rowProcessed = context.outputInfo.get("rows_processed").getOrElse("n/a")
        val end = System.currentTimeMillis()
        val appName = context.inputConfig("app_name").toString.toLowerCase
        val outputPath = context.inputConfig("output_log").toString + s"/process_status=${processStatus}/last_processed_date=${lastProcessDate}"
        val appType = getAppType(appName)

        // always append the metrics to the file location
        import context.sparkSession.implicits._
        appendDFToJSON(
          Seq(SourceJobMetric(appName, appType, processStatus,
            formatDateForElasticTime(start), formatDateForElasticTime(end), (end - start), lastProcessDate, rowProcessed))
            .toDF().repartition(1), outputPath
        )
      } catch {
        case e: Exception => processLogger.error("ProcessInterceptor ERROR is captured: " + e.getMessage)
      }
    }
  }
}
