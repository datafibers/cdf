package com.datafibers.cdf.utils

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.temporal.TemporalAdjusters
import java.util.Date
import scala.collection.convert.WrapAsScala
import scala.collection.Map

trait HelpFunc {

  def headArray[A](as: Array[A], default: String = "") = if (as.length > 0) as.head.toString else default

  def headList[A](as: List[A], default: String = "") = if (as.length > 0) as.head.toString else default

  def formatDateForTime(date: Long) = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").format(new Date(date))

  def formatDateForElasticTime(date: Long) = formatDateForTime(date).replaceAll(" ", "T")

  /**
   * Convert all possible date format to yyyy-MM-dd
   * @param d
   * @param dateFormat
   * @return
   */
  def dateScrubber(d: String, dateFormat: String = "yyyy-MM-dd"): String = {
    val reg1 = "([0-9]{4}-[0-9]{2}-[0-9]{2})".r
    val reg2 = "([0-9]{4}_[0-9]{2}_[0-9]{2})".r
    val reg3 = "([0-9]{8})".r
    val reg4 = "([0-9]{6})".r
    val reg5 = "([0-9]{4})".r
    val reg6 = "([0-9]{2}-[0-9]{2}-[0-9]{2})".r
    val reg7 = "([0-9]{2}/[0-9]{2}/[0-9]{2})".r
    val reg8 = "([0-9]{2}/[0-9]{2}/[0-9]{4})".r
    val reg9 = "([a-zA-Z]{3}_[0-9]{4})".r
    val reg10 = "([0-9]{4}_[0-9]{2})".r
    val reg11 = "([0-9]{4}-[0-9]{2})".r
    val reg12 = "([0-9]{2}-[0-9]{2}-[0-9]{4})".r
    val reg13 = "([a-zA-Z]{3} [0-9]{1,2} [0-9]{4})".r

    // datetime
    val datetimeReg1 = "([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}) ([0-9]{2}:[0-9]{2}:[0-9]{2}) (am|AM|pm|PM)".r
    val datetimeReg2 = "([0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{1,3})".r
    val datetimeReg3 = "([0-9]{4}[0-9]{2}[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})".r
    val datetimeReg4 = "([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.\\d+Z)".r
    val datetimeReg5 = "([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.\\d+).*".r
    val datetimeReg6 = "([0-9]{4}-[0-9]{2}-[0-9]{2}) ([0-9]{2}:[0-9]{2}:[0-9]{2})".r
    val datetimeReg7 = "([0-9]{4}[0-9]{2}[0-9]{2}T[0-9]{2}[0-9]{2}[0-9]{2}\\.[0-9]{3} [A-Z]{3})".r
    val datetimeReg8 = "([0-9]{2}/[0-9]{2}/[0-9]{4}) ([0-9]{2}:[0-9]{2}:[0-9]{2})".r
    val datetimeReg9 = "([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}+Z)".r

    val date = d match {
      case reg1(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("yyyy-MM-dd").parse(d))
      case reg2(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("yyyy_MM_dd").parse(d))
      case reg3(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("yyyyMMdd").parse(d))
      case reg4(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("yyyyMM").parse(d))
      case reg5(d) => LocalDate.parse(new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("yyyy").parse(d))).`with`(TemporalAdjusters.lastDayOfYear()).toString
      case reg6(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("MM-dd-yy").parse(d))
      case reg7(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("dd/MM/yy").parse(d))
      case reg8(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("dd/MM/yyyy").parse(d))
      case reg9(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("MMM_yyyy").parse(d))
      case reg10(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("yyyy_MM").parse(d))
      case reg11(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("yyyy-MM").parse(d))
      case reg12(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("MM-dd-yyyy").parse(d))
      case reg13(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("MMM dd yyyy").parse(d))
      case datetimeReg1(date, time, a) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("MM/dd/yyyy HH:mm:ss a").parse(d))
      case datetimeReg2(date, time) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.s").parse(d))
      case datetimeReg3(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("yyyyMMdd'T'HH:mm:ss").parse(d))
      case datetimeReg4(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(d))
      case datetimeReg5(_) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(d))
      case datetimeReg6(date, time) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(d))
      case datetimeReg7(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSS z").parse(d))
      case datetimeReg8(date, time) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").parse(d))
      case datetimeReg9(d) => new SimpleDateFormat(dateFormat).format(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(d))
    }
    date
  }

  def customDateScrubber(d: String, inFormat: String, outFormat: String= "yyyy-MM-dd"): String = {
    new SimpleDateFormat(outFormat).format(new SimpleDateFormat(inFormat).parse(d))
  }

  // common config parser functions

  def getAliasFromConfig(sourceProperties: Map[String, String]): String = {
    sourceProperties.getOrElse("alias", getTableFromConfig(sourceProperties))
  }

  def getTableFromConfig(sourceProperties: Map[String, String]): String = {
    val tablePre = sourceProperties.getOrElse("table", AppDefaultConfig.DEFAULT_CONF_VALUE)
    val table = if (tablePre.contains(".")) tablePre.split("\\.")(1) else tablePre
    table
  }

  def getDatabaseFromConfig(sourceProperties: Map[String, String]): String = {
    val tablePre = sourceProperties.getOrElse("table", AppDefaultConfig.DEFAULT_CONF_VALUE)
    val database = if (tablePre.contains(".")) tablePre.split("\\.")(0) else sourceProperties.getOrElse("database", AppDefaultConfig.DEFAULT_CONF_DB_NAME)
    database
  }

  def getPathFromConfig(inputConfig: Map[String, Any], sourceAlias: String): String = {
    val inputSource = inputConfig.get("source").get.asInstanceOf[java.util.List[java.util.Map[String, Any]]]
    val scalaList = WrapAsScala.asScalaBuffer(inputSource).toList
    headList(
      scalaList.map({
        case x: java.util.Map[String, Any] =>
          val config = WrapAsScala.mapAsScalaMap(x).asInstanceOf[Map[String, String]]
          val path = config.getOrElse("path", AppDefaultConfig.DEFAULT_CONF_FILE_PATH)
          val alias = config.getOrElse("alias", AppDefaultConfig.DEFAULT_CONF_TABLE_TYPE)
          if (alias == sourceAlias) path else null
      }).filter(_ != null)
    )
  }

  def getOptionalFieldByKey(optionalFieldsKeys: String): String = {
    val schema = ""
    val schemaAll = optionalFieldsKeys.split(",").foldLeft(schema)((s, p) => {
      if (AppDefaultConfig.OPTIONAL_FIELDS_MAP.contains(p))
        s + "," + AppDefaultConfig.OPTIONAL_FIELDS_MAP.getOrElse(p, "")
      else {
        println(s"${p} is not existed in AppDefaultConfig.OPTIONAL_FIELDS_MAP")
        s
      }
    })
    schemaAll.replaceFirst(",", "")
  }
}