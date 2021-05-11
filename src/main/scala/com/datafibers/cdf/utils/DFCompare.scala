package com.datafibers.cdf.utils

import com.datafibers.cdf.Main.sparkEnvInitialization
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DFCompare {

  def dfReadAndCompare(expectDFPath: String, actualDFPath: String, joinKey: String, detailThresholdAndWhere: String = "0")(implicit spark: SparkSession): String = {
    val fileTypeExpect = if (expectDFPath.contains("@")) expectDFPath.split("@")(0) else "csv"
    val fileTypeActual = if (actualDFPath.contains("@")) expectDFPath.split("@")(0) else "csv"
    val expectDFPathCleaned = if (expectDFPath.contains("@")) expectDFPath.split("@")(1) else expectDFPath
    val actualDFPathCleaned = if (actualDFPath.contains("@")) actualDFPath.split("@")(1) else actualDFPath

    val detailThreshold = if (detailThresholdAndWhere.contains(",")) detailThresholdAndWhere.split(",")(0) else detailThresholdAndWhere
    val whereClause = if (detailThresholdAndWhere.contains(","))
      detailThresholdAndWhere.split(",")(1) + " between '" +
        detailThresholdAndWhere.split(",")(2).replaceAll("'", "") + "' and '" +
        detailThresholdAndWhere.split(",")(3).replaceAll("'", "") + "'" else "1 = 1"
    val resString = StringBuilder.newBuilder
    val limitCnt = "0"
    val joinKeyStatement =
      if (joinKey.contains("="))
        joinKey.split(",").map("e." + _.replace("=", "=a.")).mkString(" and ")
      else s"a.${joinKey} = e.${joinKey}"

    val joinKeyListActual =
      if (joinKey.contains("="))
        joinKey.split(",").map("a." + _.split("=")(1)).mkString(",")
      else s"a.${joinKey}"

    val limitClause = if (limitCnt.isEmpty || limitCnt == null || limitCnt.equalsIgnoreCase("0"))
      "" else s"limit ${limitCnt}"

    println(s"detailThreshold=${detailThreshold}, whereClause=${whereClause}, joinKeyStatement=${joinKeyStatement}, " +
      s"joinKeyListActual=${joinKeyListActual}, limitClause=${limitClause}")

    val expectDF = readFile(fileTypeExpect, expectDFPathCleaned).where(whereClause)
    val actualDF = readFile(fileTypeActual, actualDFPathCleaned).where(whereClause)

    expectDF.persist()
    actualDF.persist()

    resString.append(s"expectDF = ${expectDFPathCleaned} \n")
    resString.append(s"actualDF = ${actualDFPathCleaned} \n")

    val expectCnt = expectDF.count
    val actualCnt = actualDF.count

    resString.append(s"expectDF.count = ${expectCnt}, actualDF.count = ${actualCnt}, joinKey = ${joinKey}, filter = ${whereClause}\n")
    resString.append("_" * 50 + "\n")
    expectDF.columns.sorted.toSeq.diff(actualDF.columns.sorted.toSeq).foreach(x => resString.append("expectDF diff actualDF columns = " + x + "\n"))
    resString.append("\n")
    actualDF.columns.sorted.toSeq.diff(expectDF.columns.sorted.toSeq).foreach(x => resString.append("actualDF diff expectDF columns = " + x + "\n"))
    resString.append("\n")

    // since table does not all space in column name, replace then
    val expectDFColRenamed = expectDF.columns.foldLeft(expectDF)((r, col) => r.withColumnRenamed(col, col.replaceAll(AppDefaultConfig.BAD_CHAR_IN_COL_NAME_PATTERN, "__")))
    val actualDFColRenamed = actualDF.columns.foldLeft(actualDF)((r, col) => r.withColumnRenamed(col, col.replaceAll(AppDefaultConfig.BAD_CHAR_IN_COL_NAME_PATTERN, "__")))
    expectDFColRenamed.createOrReplaceTempView("df_expect")
    actualDFColRenamed.createOrReplaceTempView("df_actual")

    // compare column again after renamed
    resString.append("_" * 50 + "\n")
    expectDFColRenamed.columns.sorted.toSeq.diff(actualDFColRenamed.columns.sorted.toSeq).foreach(x => resString.append("expectDF diff actualDF columns = " + x + "\n"))
    resString.append("\n")
    actualDFColRenamed.columns.sorted.toSeq.diff(expectDFColRenamed.columns.sorted.toSeq).foreach(x => resString.append("actualDF diff expectDF columns = " + x + "\n"))
    resString.append("\n")

    val colsComm = actualDFColRenamed.columns.sorted.toSeq.intersect(expectDFColRenamed.columns.sorted.toSeq)
    val colList = colsComm.foldLeft(new StringBuilder) { (sb, s) =>
      sb.append("a.").append(s).append(" as ").append(s).append("_a,\n").append("e.").append(s)
        .append(" as ").append(s).append("_e,\n")
    }.toString

    val mrSql = "select \n" + colList + s"${joinKeyListActual} \nfrom df_actual a join df_expect e on ${joinKeyStatement} ${limitClause}"
    println(s"matching sql start================\n${mrSql}\nmatching sql end================\n")
    val tar = spark.sql(mrSql)
    tar.createOrReplaceTempView("tar")
    val aggList = colsComm.foldLeft(new StringBuilder) { (sb, s) =>
      sb.append("sum(if(lower(trim(nvl(").append(s).append("_a,'null'))) = lower(trim(nvl(").append(s)
        .append("_e,'null'))), 1, 0))/count(*) as mr_").append(s).append(",\n")
    }.toString
    val aggSql = s"select count(*) as cnt, \n" + aggList + "1 as expect \nfrom tar"
    println(s"matching sql start================\n${aggSql}\nmatching sql end================\n")
    val mr = spark.sql(aggSql)
    mr.createOrReplaceTempView("mr")

    // pivot the matching rate for better reading format
    val colListMr = colsComm.map("mr_" + _).mkString(",")
    val colNameMr = colsComm.map("'" + _ + "'").mkString(",")
    val pivotSql = s"with b as (select posexplode(array(${colListMr})) as (pos, val) from mr), " +
      s"a as (select posexplode(array(${colNameMr})) as (pos, val) from mr) " +
      s"select a.val as column_name, b.val as match_rate from b join a on a.pos = b.pos order by match_rate, column_name"
    val pivotResult = spark.sql(pivotSql)
    pivotResult.createOrReplaceTempView("pr")
    resString.append(showString(pivotResult, 500) + "\n")

    // show compare summary/overview section
    val comOvSql =
      s"""
         |with row_cnt as (
         |select cnt as total_rows_compared from mr
         |)
         |select
         |sum(if(nvl(match_rate, 0) != 1, 0, 1)) as passed_col_name,
         |count(*) as total_col_num,
         |avg(if(column_name not like 'best_%' and column_name not like 'tresataId_%', match_rate, 1)) as match_rate_avg,
         |concat(cast(sum(if(nvl(match_rate, 0) = 1, 1, 0)) * 100/count(*) as decimal(6, 3)), '%') as pass_rate,
         |concat(cast(sum(if(nvl(match_rate, 0) = 1 and column_name not like 'best_%' and column_name not like 'tresataId_%', 1, 0)) * 100/sum(if(column_name not like 'best_%' and column_name not like 'tresataId_%', 1, 0)) as decimal(6, 3)), '%') as pass_rate_without_best_cols,
         |${expectCnt} as total_rows_expect,
         |${actualCnt} as total_rows_actual,
         |max(row_cnt.total_rows_compared) as total_rows_compared,
         |concat(cast(max(row_cnt.total_rows_compared) * 100/${expectCnt} as decimal(6,3)), '%') as expect_coverage,
         |concat(cast(max(row_cnt.total_rows_compared) * 100/${actualCnt} as decimal(6,3)), '%') as actual_coverage
         |from pr cross join row_cnt
         |""".stripMargin

    resString.append(showString(spark.sql(comOvSql)) + "\n")

    val joinKeyWithoutAlias = joinKeyListActual.replaceAll("a\\.", "")
    // here we print details for unmatched rows (top 20 rows)
    val detailSql =
      s"""
         |select sql from (
         |select concat('select distinct ', column_name, '_e, ', column_name, '_a from tar where ', column_name, '_e <> ', column_name, '_a') as sql,
         |match_rate, column_name, 1 as rn from pr
         |where match_rate < ${detailThreshold} and column_name not like 'best_%' and column_name not like 'tresataId_%'
         |union all
         |select concat('select ${joinKeyWithoutAlias}, ', column_name, '_e, ', column_name, '_a from tar where ', column_name, '_e <> ', column_name, '_a limit 20') as sql,
         |match_rate, column_name, 2 as rn from pr
         |where match_rate < ${detailThreshold} and column_name not like 'best_%' and column_name not like 'tresataId_%'
         |) order by match_rate, column_name, rn
         |""".stripMargin

    if (!detailThreshold.equalsIgnoreCase("0")) {
      spark.sql(detailSql).collect.map(row => row.getString(0)).foreach(
        x => {
          resString.append(x + "\n")
          resString.append(showString(spark.sql(x)) + "\n")
        }
      )
    }
    resString.toString
  }

  /**
   * This is implementation for spark show function
   * @param df
   * @param _numRows
   * @param truncate
   * @return
   */
  def showString(df: DataFrame, _numRows: Int = 20, truncate: Int = 100): String = {
    val numRows = _numRows.max(0)
    val takeResult = df.take(numRows + 1)
    val hasMoreData = takeResult.length > numRows
    val data = takeResult.take(numRows)

    // for array value, replace Seq and Array with square buckets
    // for cells that are beyond 'truncate' characters, replace it with the first `truncate-3` and "..."
    val rows: Seq[Seq[String]] = df.schema.fieldNames.toSeq +: data.map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case array: Array[_] => array.mkString("[", " ", "]")
          case seq: Seq[_] => seq.mkString("[", " ", "]")
          case _ => cell.toString
        }
        if (truncate > 0 && str.length > truncate) {
          // do not show ellipses for strings shorter than 4 characters
          if (truncate < 4) str.substring(0, truncate)
          else str.substring(0, truncate - 3) + "..."
        } else {
          str
        }
      } : Seq[String]
    }

    val sb = new StringBuilder
    val numCols = df.schema.fieldNames.length
    // initialise the width of each column to a minimum value of '3'
    val colWidths = Array.fill(numCols)(3)
    // computer the width of each column
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), cell.length)
      }
    }
    // create separateLine
    val sep = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString
    // column names
    rows.head.zipWithIndex.map { case (cell, i) =>
      if (truncate > 0)
        StringUtils.leftPad(cell, colWidths(i))
      else
        StringUtils.rightPad(cell, colWidths(i))
    }.addString(sb, "|", "|", "|\n")

    sb.append(sep)

    // data
    rows.tail.map {
      _.zipWithIndex.map { case(cell, i) =>
        if (truncate > 0)
          StringUtils.leftPad(cell, colWidths(i))
        else
          StringUtils.rightPad(cell, colWidths(i))
      }.addString(sb, "|", "|", "|\n")
    }

    sb.append(sep)

    // for data that has more than "numRows" records
    if(hasMoreData) {
      val rowString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowString\n")
    }

    sb.toString
  }

  def writeToHdfsFile(content: String, filePath: String) = {
    val outputFile = FileSystem.get(new Configuration()).create(new Path(filePath))
    outputFile.write(content.getBytes("UTF-8"))
    outputFile.close()
  }

  def readFile(fileTypeExpect: String, filePath: String)(implicit spark: SparkSession): DataFrame = {
    if (fileTypeExpect.contains("parq")) spark.read.parquet(filePath)
    else if (fileTypeExpect.contains("json")) spark.read.json(filePath)
    else if (fileTypeExpect.contains("orc")) spark.read.orc(filePath)
    else spark.read.format("csv").option("delimiter", "|").option("header", "true").load(filePath)
  }

  def main(args: Array[String]) = {
    val spark = sparkEnvInitialization(this.getClass.getName)
    val resultString = dfReadAndCompare(args(0), args(1), args(2), args(3))(spark)
    writeToHdfsFile(resultString, args(4))
  }

}
