package com.datafibers.cdf

import com.datafibers.cdf.utils.{SetupFunc, SourceToDS}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import java.io.File
@RunWith(classOf[JUnitRunner])
class TestCDF extends FunSuite with SourceToDS with BeforeAndAfterEach with SetupFunc {

  implicit val spark = sparkEnvInitialization(this.getClass.getName)
  System.setProperty("DE_OUTPUT_ROOT_PATH", "output")
  System.setProperty("DE_LOG_ROOT_PATH", "output/log")

  val outputFileDirectory = System.getProperty("DE_OUTPUT_ROOT_PATH")
  FileUtils.deleteDirectory(new File(outputFileDirectory))

  ignore ("row count check using empty yml config") {
    val appCode = "empty-cob"
    val args = Array(s"src/main/resources/conf/app_${appCode}.yml", "cob")
    val config = setAppConfig(args)
    spark.sql("create database if not exists kdb_uk_prod")
    spark.sql("use kdb_uk_prod")
    readDataFromFileAsDF("csv", "src/test/resources/data/kdb_uk_prod/spot_rate")
      .write.mode(SaveMode.Overwrite).saveAsTable("spot_rate")

    setAppRun(args, spark)
    val actualDFCnt = readDataFromFileAsDF(config.getOrElse("output_type", "").toString, outputFileDirectory).count
    val expectDFCnt = spark.sql("select * from kdb_uk_prod.spot_rate").count
    spark.sql("show databases")
    spark.sql("select count(*) as cnt from kdb_uk_prod.spot_rate").show
    assert(actualDFCnt === expectDFCnt)
    spark.sql("drop database if exists kdb_uk_prod cascade")
  }

  test ("row count check with file source and init sql") {
    val appCode = "file-cob"
    val cob = "20201019"
    System.setProperty("cob", s"${cob}") // since the yml file contains ${cob}, it should be in sys.properties to substitute
    val args = Array(s"src/main/resources/conf/app_${appCode}.yml", s"${cob}")
    setAppRun(args, spark)
    val actualDFCnt = spark.read.parquet(s"output/direct-insert/run_date=${cob}").count
    val expectDFCnt = readDataFromFileAsDF("csv", s"src/test/resources/data/ftek_us_prod/${cob}").count
    assert(actualDFCnt === expectDFCnt)
  }

}
