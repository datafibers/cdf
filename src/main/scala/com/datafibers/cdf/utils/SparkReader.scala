package com.datafibers.cdf.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import scala.collection.Map

trait SparkReader extends HelpFunc with SparkSQLFunc {

  /**
   * Read generic files and create alias temp view
   * @param fileProperties
   * @param spark
   */
  def readFromFile(fileProperties: Map[String, String] = AppDefaultConfig.DEFAULT_FILE_INPUT_PROS)(implicit spark: SparkSession) = {
    val alias = getAliasFromConfig(fileProperties)
    val dataFilePath = fileProperties.getOrElse("path", AppDefaultConfig.DEFAULT_CONF_FILE_PATH)
    val fileType = fileProperties.getOrElse("format", "csv")
    val readStrategy = fileProperties.getOrElse("read_strategy", AppDefaultConfig.DEFAULT_CONF_READ_STRATEGY)

    // when cob is specified in path but not provided in cml, it got default value, but removed by reader.
    val dataFilePathMatched =
      if (dataFilePath.contains("*") && readStrategy.equalsIgnoreCase("latest"))
      FileSystem.get(new Configuration())
        .globStatus(new Path(dataFilePath.replace(AppDefaultConfig.DEFAULT_APP_DATE, "")))
        .reverse.head.getPath.toString
      else dataFilePath.replace(AppDefaultConfig.DEFAULT_APP_DATE, "")

    // rewrite cob only when it is not specified during input
    if (System.getProperty("cob").equalsIgnoreCase(AppDefaultConfig.DEFAULT_APP_DATE)) {
      val regexCob = ".*/(\\d{4}-\\d{2}-\\d{2}).*".r // extract cob date from path if possible
      val cobExtract = dataFilePathMatched match {
        case regexCob(path) => path
        case _ => AppDefaultConfig.DEFAULT_APP_DATE
      }
      System.setProperty("cob", cobExtract.toString)
    }
    println(s"File read from dataFilePathMatched = ${dataFilePathMatched}")
    readDataFromFileAsDF(fileType, dataFilePathMatched, fileProperties).persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView(alias)
  }

  /**
   * Read generic files
   * @param fileType
   * @param filePath
   * @param fileProperties
   * @param spark
   * @return
   */
  def readDataFromFileAsDF(fileType: String, filePath: String, fileProperties: Map[String, String] = AppDefaultConfig.DEFAULT_FILE_INPUT_PROS)(implicit spark: SparkSession): DataFrame = {
    val fileDF =
      if (fileType.equalsIgnoreCase("parquet")) spark.read.parquet(filePath)
      else if (fileType.equalsIgnoreCase("json")) spark.read.json(filePath)
      else readFromCSVAsDF(filePath, fileProperties)

      if (fileProperties.contains("optional_fields") || fileProperties.contains("optional_fields_key")) {
        val optionalFields = fileProperties.getOrElse("optional_fields",
          getOptionalFieldByKey(
            if (fileProperties("optional_fields_key") == "alias") "tresata,best," + fileProperties("alias") // if optional_fields_key = alias, use tresata,best,alias
            else fileProperties.getOrElse("optional_fields_key", "default")
          )
        )
        optionalFields.trim.replaceAll("\n", "").split(",").foldLeft(fileDF)((r, col) => {
          if (r.columns.contains(col)) r else r.withColumn(col, lit(AppDefaultConfig.OPTIONAL_FIELDS_REPLACE))
        })
      } else fileDF
  }

  /**
   * Read CSV and register as temp view
   * @param fileProperties
   * @param spark
   */
  def readFromCSV(fileProperties: Map[String, String] = AppDefaultConfig.DEFAULT_FILE_INPUT_PROS)(implicit spark: SparkSession) = {
    val alias = getAliasFromConfig(fileProperties)
    val dataFilePath = fileProperties.getOrElse("path", AppDefaultConfig.DEFAULT_CONF_FILE_PATH)
    readFromCSVAsDF(dataFilePath, fileProperties).persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView(alias)
  }

  /**
   * Read csv files as dataframe
   * @param dataFilePath
   * @param fileProperties
   * @param spark
   * @return
   */
  def readFromCSVAsDF(dataFilePath: String, fileProperties: Map[String, String] = AppDefaultConfig.DEFAULT_FILE_INPUT_PROS)(implicit spark: SparkSession): DataFrame = {
    val hasFooterRowCount = fileProperties.getOrElse("hasFooterRowCount", AppDefaultConfig.DEFAULT_FILE_INPUT_PROS("hasFooterRowCount")).toBoolean
    val hasExtraHeader = fileProperties.getOrElse("hasExtraHeader", AppDefaultConfig.DEFAULT_FILE_INPUT_PROS("hasExtraHeader")).toBoolean
    val hasHeader = fileProperties.getOrElse("header", AppDefaultConfig.DEFAULT_FILE_INPUT_PROS("header")).toBoolean
    val extraHeaderRowCount = fileProperties.getOrElse("extraHeaderRowCount", AppDefaultConfig.DEFAULT_FILE_INPUT_PROS("extraHeaderRowCount")).toInt
    val headerTrailerInclusiveFooter = fileProperties.getOrElse("headerTrailerInclusiveFooter", AppDefaultConfig.DEFAULT_FILE_INPUT_PROS("headerTrailerInclusiveFooter")).toBoolean
    val delimiter = fileProperties.getOrElse("delimiter", AppDefaultConfig.DEFAULT_FILE_INPUT_PROS("delimiter"))
    val quote = fileProperties.getOrElse("quote", AppDefaultConfig.DEFAULT_FILE_INPUT_PROS("quote"))
    val regex = "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)" // this pattern splits on delimiter but ignore within commas

    val schema =
      if (!hasHeader) { // no header, read schema from header file
        val headerDelimiter = fileProperties.getOrElse("header_delimiter", delimiter)
        val header = spark.read.textFile(dataFilePath + ".header").first
        StructType(header.split("\\" + headerDelimiter + regex, -1).map(_.replace(quote, "")).map(x => StructField(x, StringType)))
      } else { // with header, read schema from header line
        val outputFile = spark.read.textFile(dataFilePath).rdd.filter(line => !line.trim.isEmpty).zipWithIndex()
        val outputFileWithoutExtraHeader = if (hasExtraHeader) outputFile.filter({case (line, index) => index >= extraHeaderRowCount}) else outputFile // remove extra header to find real header having schema
        val header = outputFileWithoutExtraHeader.first._1
        StructType(header.split("\\" + delimiter + regex, -1).map(_.replace(quote, "")).map(x => StructField(x, StringType)))
      }

    // multiple delimiters are supported by spark 3.0 SPARK-24540
    // for now, we deal with standalone csv parser to replace multiple delimiter to default one
    val inputDataReader = spark.read.format("csv").option("quote", quote).option("header", hasHeader)
      .option("inferSchema", fileProperties.getOrElse("inferSchema", AppDefaultConfig.DEFAULT_FILE_INPUT_PROS("inferSchema")))
      .option("ignoreLeadingWhiteSpace", fileProperties.getOrElse("ignoreLeadingWhiteSpace", AppDefaultConfig.DEFAULT_FILE_INPUT_PROS("ignoreLeadingWhiteSpace")))
      .option("ignoreTrailingWhiteSpace", fileProperties.getOrElse("ignoreTrailingWhiteSpace", AppDefaultConfig.DEFAULT_FILE_INPUT_PROS("ignoreTrailingWhiteSpace")))
      .schema(schema)

    val inputData = if (delimiter.length > 1)
      inputDataReader.option("delimiter", AppDefaultConfig.DEFAULT_FILE_INPUT_PROS("delimiter")).csv(replaceMultipleDelimiter(dataFilePath, delimiter, AppDefaultConfig.DEFAULT_FILE_INPUT_PROS("delimiter")))
    else
      inputDataReader.option("delimiter", delimiter).load(dataFilePath)

    val inputDataFiltered = if (hasExtraHeader || hasFooterRowCount) { // filter extra header and footer if it has
      val inputDataRddWithIndex = inputData.rdd.zipWithIndex()
      val inputDataRddWithIndexFilterFooter = if (hasFooterRowCount) { // whenever it has footer, we do row count validation as well as footer filtering
        // get row count form footer
        val footerDetails = inputDataRddWithIndex.sortBy(x => x._2, false).first
        val footerIndex = footerDetails._2
        val expectedCount = footerDetails._1.get(0).toString.replaceAll("[-+.^:,]", "").toInt
        // get actual row count from rdd
        val inputDataRowCount = inputDataRddWithIndex.count
        val actualCount = if (headerTrailerInclusiveFooter) inputDataRowCount else if (hasExtraHeader) inputDataRowCount - extraHeaderRowCount - 1 else inputDataRowCount - 1
        // row count validation
        assert(actualCount == expectedCount, s"Row Count validation failed, expected rows ${expectedCount} but actually ${actualCount}")
        // footer filter
        inputDataRddWithIndex.filter(x => x._2 != footerIndex)
      } else inputDataRddWithIndex

      val inputDataRddWithIndexFilterFooterAndExtraHeader = if (hasExtraHeader) {
        inputDataRddWithIndexFilterFooter.filter({case(line, index) => index >= extraHeaderRowCount}) // as index starts from 0
      } else inputDataRddWithIndexFilterFooter

      val filteredRdd = inputDataRddWithIndexFilterFooterAndExtraHeader.map({case(line, index) => line}) // remove index filed
      spark.createDataFrame(filteredRdd, inputData.schema)
    } else inputData
    inputDataFiltered
  }

  /**
   * Read file and replace multiple delimiters in it by specified delimiters
   * @param filePath
   * @param delimiter
   * @param replacedDelimiter
   * @param spark
   * @return
   */
  @deprecated("This is deprecated since Spark 3.0.0")
  def replaceMultipleDelimiter(filePath: String, delimiter: String, replacedDelimiter: String)(implicit spark: SparkSession): Dataset[String] = {
    val outputRdd = spark.read.textFile(filePath).rdd.filter(line => !line.trim.isEmpty).map(x => x.replace(delimiter, replacedDelimiter))
    import spark.implicits._
    spark.createDataset(outputRdd)
  }

  /**
   * Read hive using spark and create alias temp view
   * @param hiveProperties
   * @param spark
   */
  def readFromHive(hiveProperties: Map[String, String])(implicit spark: SparkSession) = {
    val table = getTableFromConfig(hiveProperties)
    val alias = getAliasFromConfig(hiveProperties)
    val database = getDatabaseFromConfig(hiveProperties)
    val regx = hiveProperties.getOrElse("regx", AppDefaultConfig.DEFAULT_CONF_VALUE)
    val R = s"""${regx}""".r
    val tableType = hiveProperties.getOrElse("type", AppDefaultConfig.DEFAULT_CONF_TABLE_TYPE)
    val readStrategy = hiveProperties.getOrElse("read_strategy", AppDefaultConfig.DEFAULT_CONF_READ_STRATEGY)
    val lookback = hiveProperties.getOrElse("lookback", hiveProperties.getOrElse("lookback_default", AppDefaultConfig.DEFAULT_CONF_LOOK_BACK))
    val oneDBName = if (regx.isEmpty) database else headArray(spark.sql(s"show databases like '*{database}*'").collect.map(row => row.getString(0)).filter(_.matches(regx)))
    val partitionCol = if (!tableType.contains("file")) headArray(spark.sql(s"desc ${oneDBName}.${table}").collect.map(row => row.getString(0)).dropWhile(!_.matches("# col_name")).filterNot(_.equalsIgnoreCase("# col_name"))) else null

    (
      if (regx.isEmpty) {
        if (!partitionCol.isEmpty) {
          readStrategy match {
            case "latest" => spark.sql(s"select * from ${database}.${table} where ${partitionCol} in (select max(${partitionCol}) from ${database}.${table})") // todo: get partition from show partitions
            case "all" => spark.sql(s"select * from ${database}.${table}")
            case _ => spark.sql(s"select * from ${database}.${table} where ${partitionCol} = '${readStrategy}'")
          }
        } else spark.sql(s"select * from ${database}.${table}")
      } else { // this is to support legacy database name with date in it
        val dbList = spark.sql(s"show databases like '*${database}*'").filter(row => row.getString(0) match {
          case R(d) => true
          case _ => false
        })
          .selectExpr("databasename", "row_number() over (order by databasename desc) as rn")
          .where(s"rn <= ${lookback}")
          .select("databasename")
          .collect.map(row => row.getString(0)).filter(_.matches(regx)).map("select * form " + _ + s"${table}")

        readStrategy match {
          case "all" => spark.sql(dbList.mkString(s" union all ")) // when dblist has ony one db mkstring does not append union all
          case _ => spark.sql(sqlText = "select * from " + dbList.sorted.reverse.head)
        }

      }
    ).persist(StorageLevel.MEMORY_AND_DISK).createOrReplaceTempView(alias)
  }

  def readFromElastic(esProperties: Map[String, String])(implicit spark: SparkSession) = {
    val idxName = esProperties.getOrElse("es.index", AppDefaultConfig.DEFAULT_CONF_ES_IDX_NAME)
    val typeName = esProperties.getOrElse("es.type", AppDefaultConfig.DEFAULT_CONF_ES_TYPE_NAME)
    val esDfReader = spark.read.format("org.elasticsearch.spark.sql")
    for((k, v) <- esProperties)
      esDfReader.option(k, v)

    esDfReader.load(s"${idxName}/${typeName}").persist(StorageLevel.MEMORY_AND_DISK)
      .createOrReplaceTempView(s"${idxName}-${typeName}")
  }

  def readFromJDBC(jdbcProperties: Map[String, String])(implicit spark: SparkSession) = {
    val jdbcUrl = jdbcProperties.getOrElse("jdbcUrl", "")
    val query = jdbcProperties.getOrElse("query", "")
    val user = jdbcProperties.getOrElse("user", "")
    val alias = getAliasFromConfig(jdbcProperties)
    val driver =
      if(jdbcUrl.contains("oracle"))
        "oracle.jdbc.driver.OracleDriver"
      else if(jdbcUrl.contains("mysql"))
        "com.mysql.jdbc.Driver"
      else if(jdbcUrl.contains("sqlserver"))
        "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      else ""

    spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", query) // change to query in spark 2.4
      .option("user", user)
      .option("password", spark.sparkContext.hadoopConfiguration.getPassword("pwdjceks").toString)
      .option("driver", driver)
      .load()
      .createOrReplaceTempView(s"${alias}")
  }


}
