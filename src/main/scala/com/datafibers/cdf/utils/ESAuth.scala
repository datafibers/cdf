package com.datafibers.cdf.utils

import com.datafibers.cdf.Main.sparkEnvInitialization

object ESAuth {
  // get elastic credential
  def main(args: Array[String]) = {
    val spark = sparkEnvInitialization(this.getClass.getName)
    print("'" +
      spark.conf.get("es.net.http.auth.user") + ":" + spark.conf.get("es.net.http.auth.pass") + "' " +
      spark.conf.get("es.nodes").split(",")(0) + ":" + spark.conf.get("es.port")
    )
  }
}
