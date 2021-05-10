package com.datafibers.cdf.core

import org.apache.spark.sql.SparkSession
import scala.collection.{Map, mutable}

case class ProcessContext(inputConfig: Map[String, Any], sparkSession: SparkSession, dryRun: Boolean = false) {
  val outputInfo: mutable.Map[String, String] = new mutable.HashMap[String, String]()
  val parametersInfo: mutable.Map[String, String] = new mutable.HashMap[String, String]()
  parametersInfo += ("dryRun" -> dryRun.toString)
}
