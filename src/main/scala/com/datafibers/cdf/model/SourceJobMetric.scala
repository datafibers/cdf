package com.datafibers.cdf.model

case class SourceJobMetric(appName: String, appType: String,
                           processStatus: String, processStart: String, processEnd: String,
                           processTime: Long, lastProcessedDate: String, rowProcessed: String)
