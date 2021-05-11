package com.datafibers.cdf.utils

import com.google.gson.{JsonObject, JsonParser}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, expr, lit, trim, udf}

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Date
import scala.io.Source.fromInputStream
import scala.collection.Map

trait SparkSQLFunc extends HelpFunc {

  val numberReg = """^-?[0-9]*([\.0-9]+)?$""".r
  val decimalNumberReg = """^[-+]?[0-9]*\.[0-9]+([eE][-+]?[0-9]+)?$""".r
  val repeatNumberReg = """\b(\d)\1+\b""".r
  val negativeNumberReg = """(-[0-9]*)""".r
  val noneDateNumberReg = """([0-9]{8}[0-9]+)""".r
  val dateLikeReg = """(\d{2,4}-.*)""".r
  val dateReg = """([0-9]{4})-([0-9]{2})-([0-9]{2})""".r

  private def toDefault(x: String, sub: String): String = x match {
    case null => sub
    case AppDefaultConfig.DEFAULT_UDF_STRING => sub
    case AppDefaultConfig.DEFAULT_UDF_FLOAT => sub
    case _ => x.toString
  }

  val removeLeadingUDF = udf {(x: String) => Option(x).map(_.replaceFirst("^[ 0]+(?=[-0-9]+)", ""))}

  val removeLeadingNegativeUDF = udf {(x: String) => Option(x).map(_.replaceFirst("^+-[0]+(?=[0-9]+)", "-"))}

  val replaceFloatNullUDF = udf {(x: String) => toDefault(x, AppDefaultConfig.DEFAULT_UDF_FLOAT)}

  val fixDateOutlierUDF = udf {(x: String) =>
    x match {
      case dateReg(first, "00", third) => AppDefaultConfig.DEFAULT_UDF_DATE
      case dateReg(first, second, "00") => AppDefaultConfig.DEFAULT_UDF_DATE
      case dateReg("0000", second, third) => AppDefaultConfig.DEFAULT_UDF_DATE
      case dateReg("0001", second, third) => AppDefaultConfig.DEFAULT_UDF_DATE
      case _ => x
    }
  }

  val dateScrubberUDF = udf {(x: String) => dateScrubber(x)}

  val customDateScrubberUDF = udf {(x: String, input: String, output: String) =>
    if (x == AppDefaultConfig.DEFAULT_UDF_DATE) x
    else customDateScrubber(x, input, output)
  }

  val toDefaultDateUDF = udf {(s: String) =>
    val len = if (s != null) s.filter(_.isDigit).length else 0
    val value = if (4 < len && len < 30) s else "NOT_VALID_DATE"
    value match {
      case negativeNumberReg(_) => AppDefaultConfig.DEFAULT_UDF_DATE
      case noneDateNumberReg(_) => AppDefaultConfig.DEFAULT_UDF_DATE
      case decimalNumberReg(_) => AppDefaultConfig.DEFAULT_UDF_DATE
      case repeatNumberReg(_) => AppDefaultConfig.DEFAULT_UDF_DATE
      case AppDefaultConfig.DEFAULT_UDF_STRING => AppDefaultConfig.DEFAULT_UDF_DATE
      case AppDefaultConfig.DEFAULT_UDF_FLOAT => AppDefaultConfig.DEFAULT_UDF_DATE
      case "NOT_VALID_DATE" => AppDefaultConfig.DEFAULT_UDF_DATE
      case _ => s
    }
  }

  val toDoubleZeroUDF = udf ((s: String) =>
    s match {
      case numberReg(_) => s
      case decimalNumberReg(_) => s
      case _ => AppDefaultConfig.DEFAULT_UDF_FLOAT
    }
  )

  /**
   * Run SQL and substitute parameters
   * @param inputConfig
   * @param sqlFilePath
   * @param spark
   * @return
   */
  def sqlRunner(inputConfig: Map[String, Any], sqlFilePath: String)(implicit spark: SparkSession): DataFrame = {
    val sqlFilePathUnified = if (sqlFilePath.startsWith("hdfs:")) sqlFilePath.replaceAll("hdfs:", "") else sqlFilePath
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val sqlScript = if (sqlFilePath.startsWith("hdfs:") && fs.exists(new Path(sqlFilePathUnified))) {
      spark.read.textFile(sqlFilePathUnified).collect.mkString("\n")
    } else {
      // here append all sql files (.sql.part{n}) when specified from inputConfig except sql in unit test
      if (inputConfig.contains("sql_file_part") && !sqlFilePath.contains("tsql_"))
        (1 to inputConfig("sql_file_part").toString.toInt)
          .map(x => fromInputStream(getClass.getResourceAsStream(sqlFilePath + ".part" + x)).getLines.mkString("\n"))
          .mkString("\n")
      else if (inputConfig.getOrElse("sql_init", "false").toString.equalsIgnoreCase("true")) {
        val sqlInitFilePath = inputConfig.getOrElse("sql_resource_root", AppDefaultConfig.DEFAULT_CONF_SQL_RESOURCE_ROOT).toString + "/init.sql"
        fromInputStream(getClass.getResourceAsStream(sqlInitFilePath)).getLines.mkString("\n") + "\n" +
          fromInputStream(getClass.getResourceAsStream(sqlFilePath)).getLines.mkString("\n")
      } else fromInputStream(getClass.getResourceAsStream(sqlFilePath)).getLines.mkString("\n")
    }

    val sqlScriptWithoutInitSQL = if (inputConfig.getOrElse("sql_init_global", "false").toString.equalsIgnoreCase("true"))
      AppDefaultConfig.GLOBAL_INIT_SQL + "\n" + sqlScript
    else sqlScript

    // replace cob and ppd with proper value in the spark sql file
    val sqlScriptCleaned = sqlScriptWithoutInitSQL
      .replaceAll("--.*\n", "\n")
      .replaceAll("\\$\\{DE_OUTPUT_ROOT_PATH}", System.getProperty("DE_OUTPUT_ROOT_PATH", AppDefaultConfig.DEFAULT_CONF_STAGE_OUTPUT_ROOT_PATH))
      .replaceAll("\\$\\{cob}", System.getProperty("cob"))
      .replaceAll("\\$\\{pdd}",  System.getProperty("ppd", AppDefaultConfig.DEFAULT_APP_DATE))

    // replace additional parameter from para_1 to para_n
    val otherPara = System.getProperty("other_para")
    var sqlScriptParaIngested = sqlScriptCleaned
    if (!otherPara.isEmpty) {
      val paraList = otherPara.split(",")
      var n = 0
      for (para <- paraList) {
        sqlScriptParaIngested = sqlScriptParaIngested.replaceAll("\\$\\{para_" + n + "}", para)
        n = n + 1
      }
    }

    println(s"sqlScriptParaIngested = ${sqlScriptParaIngested}")

    sqlScriptParaIngested.split(";").filterNot(_.equalsIgnoreCase("\n\n")).map(
      query => spark.sql(
        if (inputConfig.getOrElse("dry_run", "false") == "true" && !query.trim.toLowerCase.startsWith("set")) // does not explain set command
          "explain " + query
        else
          query
      )).reverse.head
  }

  /**
   * Find the latest processed date from sql hint showing as driver table
   * @param sqlFilePath
   * @param spark
   * @return
   */
  def findLastProcessFromSQL(sqlFilePath: String)(implicit spark: SparkSession): String = {
    val sqlFilePathUnified = if (sqlFilePath.startsWith("hdfs:")) sqlFilePath.replaceAll("hdfs:", "") else sqlFilePath
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val sqlScript = if (sqlFilePath.startsWith("hdfs:") && fs.exists(new Path(sqlFilePathUnified))) {
      spark.read.textFile(sqlFilePathUnified).collect.mkString("\n")
    } else {
      fromInputStream(getClass.getResourceAsStream(sqlFilePathUnified)).getLines.mkString("\n")
    }

    val dbTableName = headArray(sqlScript.split("\n").filter(_.replaceAll(" ", "")
      .contains("--driver"))).replaceAll(" ", "").replace("--driver", "")

    val partitionCol = headArray(spark.sql(s"desc ${dbTableName}")
      .collect.map(row => row.getString(0))
      .dropWhile(!_.matches("# col_name")).filterNot(_.equalsIgnoreCase("# col_name")))

    if (!partitionCol.isEmpty)
      headArray(spark.sql(s"select max(${partitionCol}) from ${dbTableName}").collect.map(row => row.getString(0)))
    else
      new SimpleDateFormat("yyyy-MM-dd").format(new Date())
  }

  def findLastProcessFromFile(sourceConfig: Map[String, String]): String = {
    val tableType = sourceConfig.getOrElse("type", AppDefaultConfig.DEFAULT_CONF_TABLE_TYPE)
    if (tableType.contains("driver")) {
      val ppd = System.getProperty("cob") // here we extract cob from path without scrubber to support runid = yyyy-MM-dd_id
      if (ppd.contains("_")) dateScrubber(ppd.split("_")(0) + "_" + ppd.split("_")(1))
      else dateScrubber(ppd)
    } else null
  }

  def findLastProcessFromHive(sourceConfig: Map[String, String])(implicit spark: SparkSession): String = {
    val table = getTableFromConfig(sourceConfig)
    val database = getDatabaseFromConfig(sourceConfig)
    val regx = sourceConfig.getOrElse("regx", AppDefaultConfig.DEFAULT_CONF_VALUE)
    val R = s"""${regx}""".r
    val tableType = sourceConfig.getOrElse("type", AppDefaultConfig.DEFAULT_CONF_TABLE_TYPE)
    val oneDBName = if (regx.isEmpty) database else headArray(spark.sql(s"show databases like '*{database}*'").collect.map(row => row.getString(0)).filter(_.matches(regx)))
    val partitionCol = if (!tableType.contains("file")) headArray(spark.sql(s"desc ${oneDBName}.${table}").collect.map(row => row.getString(0)).dropWhile(!_.matches("# col_name")).filterNot(_.equalsIgnoreCase("# col_name"))) else null

    if (regx.isEmpty) {
      if (tableType.contains("driver")) {
        if (!partitionCol.isEmpty)
          headArray(spark.sql(s"select max(${partitionCol}) from ${database}.${table}")
            .collect.map(row => row.getString(0)).filter(_.matches(regx)))
        else new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      } else null
    } else {
      val lastDBName = headArray(spark.sql(s"show databases like '*${database}*'")
        .filter(row => row.getString(0) match {
          case R(d) => true
          case _ => false
        })
        .selectExpr("databasename", "row_number() over (order by databasename desc) as rn")
        .where(s"rn = 1")
        .select("databasename")
        .collect.map(row => row.getString(0))
      )

      lastDBName match {
        case R(d) => dateScrubber(d)
        case _ => null
      }
    }
  }

  def applyTableRowFilter(sourceConfig: Map[String, String])(implicit spark: SparkSession) = {
    val table = getTableFromConfig(sourceConfig)
    val alias = getAliasFromConfig(sourceConfig)
    val rowFilter = sourceConfig.getOrElse("row_filter", AppDefaultConfig.DEFAULT_CONF_VALUE)
    if (!rowFilter.isEmpty) spark.sql(
      s"with base as (select *, row_number() over (${rowFilter}) as rn from ${alias}) select * from base where rn = 1"
    ).createOrReplaceTempView(alias)
  }

  /**
   * Get the last_process_date from all previously successful runs
   * @param appCode
   * @param spark
   * @return
   */
  def getMetricsPreviousProcessedDate(appCode: String)(implicit spark: SparkSession): String = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val logFilePath = new Path(
      System.getProperty("DE_LOG_ROOT_PATH", AppDefaultConfig.DEFAULT_CONF_STAGE_LOG_ROOT_PATH) +
        s"app_code=${appCode}/process_status=successful/"
    )
    if (fs.exists(logFilePath))
      fs.listStatus(logFilePath).map(x => x.getPath.toString).map(x => x.substring(x.lastIndexOf("=") + 1)).sortBy(
        x => if (x.contains("_")) dateScrubber(x.split("_")(0)) + "_" + x.split("_")(1) else dateScrubber(x)
      ).reverse.head
    else
      AppDefaultConfig.DEFAULT_APP_DATE
  }

  /**
   * Clean and format all date to standard format
   * @param inputConfig
   * @param inputDF
   * @param spark
   * @return
   */
  def genericDateCleaner(inputConfig: Map[String, Any], inputDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    if (inputConfig.contains("cleaned_date_fields")) {
      val dateFields = inputConfig.getOrElse("cleaned_date_fields", "").toString.split(",")
      val inputFormat = inputConfig.getOrElse("cleaned_date_input_format", null)
      val outputFormat = inputConfig.getOrElse("cleaned_date_output_format", "yyyy-MM-dd")

      inputDF.columns.foldLeft(inputDF)((r, c) => {
        if (dateFields.contains(c)) {
          if (inputFormat == null) r.withColumn(c, fixDateOutlierUDF(dateScrubberUDF(toDefaultDateUDF(trim(col(c))))))
          else r.withColumn(c, fixDateOutlierUDF(customDateScrubberUDF(toDefaultDateUDF(trim(col(c))), lit(inputFormat), lit(outputFormat))))
        } else r
      })
    } else inputDF
  }

  /**
   * Clean amount fields to the standard format.
   * @param inputConfig
   * @param inputDF
   * @param spark
   * @return
   */
  def genericAmountCleaner(inputConfig: Map[String, Any], inputDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    if (inputConfig.contains("cleaned_amount_fields")) {
      val amountFields = inputConfig.getOrElse("cleaned_amount_fields", "").toString.split(",")

      inputDF.columns.foldLeft(inputDF)((r, c) => {
        if (amountFields.contains(c)) {
          r.withColumn(c, toDoubleZeroUDF(removeLeadingNegativeUDF(removeLeadingUDF(replaceFloatNullUDF(col(c))))))
        } else r
      })
    } else inputDF
  }

  /**
   * Clean null value as configured.
   * @param inputConfig
   * @param inputDF
   * @param spark
   * @return
   */
  def genericNullCleaner(inputConfig: Map[String, Any], inputDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    if (inputConfig.contains("cleaned_null_as")) {
      val empty = inputConfig.getOrElse("cleaned_null_as", AppDefaultConfig.DEFAULT_UDF_STRING)

      inputDF.columns.foldLeft(inputDF)((r, c) => {
        r.withColumn(c, expr(s"if(trim(nvl(`$c`, '')) == '', '$empty', '$c')"))
      })
    } else inputDF
  }

  /**
   * Append etl metadata to the dataframe.
   * @param inputConfig
   * @param inputDF
   * @param spark
   * @return
   */
  def etlMetaAppender(inputConfig: Map[String, Any], inputDF: DataFrame)(implicit spark: SparkSession): DataFrame = {
    if (inputConfig.contains("etl_meta_enabled") && inputConfig.getOrElse("etl_meta_enabled", "false") == "true") {
      inputDF.columns.foldLeft(inputDF)((r, c) => {
        r
          .withColumn("etl_pkey", expr("uuid()"))
          .withColumn("etl_app_code", lit(inputConfig("app_code")))
          .withColumn("etl_start_time", lit(inputConfig("etl_start_time").toString))
          .withColumn("etl_end_time", lit(LocalDateTime.now().toString))
      })
    } else inputDF
  }

  /**
   * Fetch schema from dataframe and create elastic mapping based on the data type.
   * todo: support more flexible of mapping, such as through sql comments after column name
   * @param inputConfig
   * @param inputDF
   * @return
   */
  def schemaToMapping(inputConfig: Map[String, Any], inputDF: DataFrame): JsonObject = {
    val defaultTypeElse = inputConfig.getOrElse("idx_type_default_else", "text:dummy").toString
    val defaultTypeOpposite = defaultTypeElse.split(":")(0)
    val defaultType = if (defaultTypeOpposite == "keyword") "text" else "keyword"
    val defaultTypeCols = defaultTypeElse.split(":")(1).split(",")
    val propertiesJson = new JsonObject
    val propertyJson = new JsonObject

    if (inputConfig.contains("idx_prop_ov")) {
      propertiesJson.add("properties", new JsonParser().parse(inputConfig("idx_prop_ov").toString))
    } else {
      inputDF.schema.filter(_.name != "etl_batch_id").foreach(schema => {
        val colTypeJson = new JsonObject
        schema.dataType.typeName match {
          case "date" => {
            colTypeJson.addProperty("type", "date")
            colTypeJson.addProperty("store", false)
            colTypeJson.addProperty("format", "yyyy-MM-dd")
          }
          case "double" | "decimal" => {
            colTypeJson.addProperty("type", "double")
            colTypeJson.addProperty("store", true)
          }
          case _ => {
            colTypeJson.addProperty("type", defaultType)
            colTypeJson.addProperty("store", true)
          }
        }
        if (defaultTypeCols.contains(schema.name)) {
          colTypeJson.addProperty("type", defaultTypeOpposite)
          colTypeJson.addProperty("store", true)
        }
        propertiesJson.add(schema.name, colTypeJson)
      })
      propertiesJson.add("properties", propertyJson)
    }

    val mapJson = new JsonObject
    propertiesJson.add("properties", propertyJson)

    if (inputConfig.contains("idx_setting"))
      mapJson.add("settings", new JsonParser().parse(inputConfig("idx_setting").toString).getAsJsonObject)

    mapJson.add("mappings", propertiesJson)
    mapJson

  }

  /**
   * Create sample data for unit test in specified path, schema, and type.
   * @param filePath
   * @param schema
   * @param fileType
   * @param spark
   */
  def createSampleDate(filePath: String, schema: String, fileType: String = "parquet")(implicit spark: SparkSession) = {
    val query = "select uuid() as `" +
      schema.replaceAll("\n", "").replaceAll(",", "`, uuid() as `") +
      "` lateral view explode(1,2,3,4,5,6,7,8,9,10)) as rows_num"
    val processDF = spark.sql(query)
    println(s"Creating sample data at ${filePath}")

    if (fileType.contains("parq")) {
      val df = processDF.columns.foldLeft(processDF)((r, col) =>
        r.withColumnRenamed(col, col.replaceAll(AppDefaultConfig.BAD_CHAR_IN_COL_NAME_PATTERN, "__")))
      df.printSchema()
    } else {
      processDF.write.mode(SaveMode.Overwrite)
        .format("csv").option("delimiter", "|").option("header", schema).save(filePath)
      processDF.printSchema()
    }
  }

  /**
   * Create sample data for unit test in specified path, pattern key, and type.
   * @param filePath
   * @param patternKey
   * @param fileType
   * @param spark
   */
  def createSampleDataWithPattern(filePath: String, patternKey: String, fileType: String = "parquet")(implicit spark: SparkSession) = {
    createSampleDate(filePath, getOptionalFieldByKey(patternKey), fileType)
  }





}
