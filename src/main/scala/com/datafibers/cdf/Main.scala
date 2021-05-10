package com.datafibers.cdf
import com.datafibers.cdf.utils.SetupFunc

object Main extends SetupFunc {

  def main(args: Array[String]) = {
    val spark = sparkEnvInitialization(this.getClass.getName)
    setAppRun(args, spark)
    spark.close()
  }
}
