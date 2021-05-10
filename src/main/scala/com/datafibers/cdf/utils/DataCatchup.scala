package com.datafibers.cdf.utils

import com.datafibers.cdf.Main.sparkEnvInitialization
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.hadoop.conf.Configuration
/**
 * If the data is not available, copy from previous day
 */
object DataCatchup {
  def catchupFiles(cob: String, filePathPattern: String) = {
    val path = new Path(filePathPattern)
    val conf = new Configuration
    val fileSys = path.getFileSystem(conf)
    val regex = ".*(\\d{4}-\\d{2}-\\d{2}).*".r

    fileSys.listStatus(path)
      .filter(_.getPath.getName.endsWith("-post"))
      .filter(!_.getPath.getName.startsWith("tides"))
      .map(fileStatus => {
        val latestPath =
          fileSys
            .listStatus(fileStatus.getPath)
            .filter(fileStatus => fileSys.getContentSummary(fileStatus.getPath).getSpaceConsumed > AppDefaultConfig.DEFAULT_CATCHUP_MIN_FILE_SIZE)
            .maxBy(_.getModificationTime).getPath
        val expectedPath = new Path(s"${fileStatus.getPath}/run_date=${cob}")
        println(s"Looking for path ${expectedPath}")
        println(s"Found latest at ${latestPath}")

        if(!fileSys.exists(expectedPath)) {
          val previousDate = latestPath.getName match {
            case regex(date) => Some(date)
            case _ => None
          }

          previousDate match {
            case Some(date) =>
              if (date == AppDefaultConfig.DEFAULT_CATCHUP_CHECK_DATE)
                println("No need to copy as this is the default path for dummy data")
              else {
                FileUtil.copy(fileSys, latestPath, fileSys, expectedPath, false, conf)
                fileSys.create(new Path(expectedPath + "/_CARRIED_OVER")).write("".getBytes("UTF-8"))
                println(s"Copied data from previous date ${date} to run_date=${cob}")
              }
            case _ => None
          }
        } else print(s"File exists ${latestPath} for run_date=${cob}")
      })
  }

  def main(args: Array[String]) = {
    val spark = sparkEnvInitialization(this.getClass.getName)
    val outputPath = args(1)
    if(outputPath == null)
      println(s"DE_OUTPUT_ROOT_PATH requires to be in env.properties to use this feature.")
    else {
      catchupFiles(args(0), outputPath)
      println(s"Data catchup is completed on ${args(0)} on ${outputPath}.")
    }
  }
}
