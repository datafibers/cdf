package com.datafibers.cdf.utils

import com.datafibers.cdf.YamlConfigReader
import com.datafibers.cdf.core.{ProcessContext, ProcessInterceptor}
import com.datafibers.cdf.process.TransformationStep
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.{JsonObject, JsonParser}
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost, HttpPut}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.auth.BasicScheme
import org.apache.http.impl.client.{BasicAuthCache, BasicCredentialsProvider, HttpClientBuilder}
import org.apache.http.util.EntityUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.io.{File, FileInputStream}
import java.net.URL
import java.time.LocalDateTime
import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.convert.WrapAsScala
import scala.util.{Failure, Success, Try}

trait SetupFunc {
  val appLogger = LoggerFactory.getLogger(this.getClass.getName)

  def setEnvProperties() = {
    // val conf = new Configuration
    // val fs = FileSystem.get(conf)
    if (new File("env.properties").exists()) { // TODO: throw exception when env file does not existed
      val is = new FileInputStream("env.properties")
      val prop = new Properties()
      prop.load(is)
      prop.entrySet().asScala.foreach {
        (entry) => {
          appLogger.warn(s"env.properties' key value is ${entry.getKey.asInstanceOf[String]}")
          sys.props += ((entry.getKey.asInstanceOf[String], entry.getValue.asInstanceOf[String]))
        }
      }
    } else
      appLogger.warn(s"Cannot find env.properties.")
  }

  def getYamlConfiguration(configurationPath: String): Map[String, Any] = {
    var configurationMap = Map[String, Any]()
    val configMap = (
      if (configurationPath != null) YamlConfigReader.readYaml(configurationPath)
      else YamlConfigReader.readYamlFromResources(getClass.getResourceAsStream("/application.yml"))).asInstanceOf[java.util.Map[String, Any]]
    WrapAsScala.mapAsScalaMap(configMap).foreach(x => configurationMap += (x._1 -> x._2))
    configurationMap
  }

  def getConfigurationByKey(key: String, configMap: Map[String, Any]): Map[String, Any] = {
    val config = configMap.get(key) getOrElse (null)
    var flatConfigMap = Map[String, Any]()
    if (config != null) {
      val configMap = config.asInstanceOf[java.util.List[java.util.Map[String, Any]]]
      val scalaList = WrapAsScala.asScalaBuffer(configMap).toList
      scalaList.foreach((w: java.util.Map[String, Any]) =>
        WrapAsScala.mapAsScalaMap(w).foreach(x => flatConfigMap += (x._1 -> x._2))
      )
    }
    flatConfigMap
  }

  def setSparkProperties(sparkConf: SparkConf, configMap: Map[String, Any]) = {
    val config = getConfigurationByKey("spark", configMap)
    val sparkMaster =
      if (System.getenv("CONFIG_SPARK_MASTER") != null)
        System.getenv("CONFIG_SPARK_MASTER")
      else {
        appLogger.warn("$CONFIG_SPARK_MASTER is not being set. Run as default " + AppDefaultConfig.DEFAULT_APP_SPARK_MASTER)
        AppDefaultConfig.DEFAULT_APP_SPARK_MASTER
      }
    sparkConf.setMaster(sparkMaster)
    for ((k, v) <- config) {
      sparkConf.set(k, v.toString)
      appLogger.warn(s"loaded spark property key=${k} value=${v}")
    }
  }

  def setElasticProperties(sparkConf: SparkConf, configMap: Map[String, Any]) = {
    if (configMap.contains("elastic")) {
      val elasticConfig = getConfigurationByKey("elastic", configMap)
      for ((k, v) <- elasticConfig) {
        sparkConf.set(k, v.toString)
        appLogger.warn(s"loaded elastic property key=${k} value=${v}")
      }
      val pass = (DecryptPasswordUtil.decryptPwdFile(sparkConf.get("es.net.http.auth.pass.path"), sparkConf.get("es.key.path")).map(_.toChar)).mkString
      val user = sys.props.getOrElse("es.net.http.auth.user", "elastic")
      sparkConf.set("es.net.http.auth.pass", pass)
      sparkConf.set("es.xpack.security.user", s"${user}:${pass}")
    }
  }

  /**
   * Initialize Spark session alone with other application properties
   * @param applicationName
   * @param configPath
   * @return sparkSession
   */
  def sparkEnvInitialization(applicationName: String, configPath: String = null): SparkSession = {
    appLogger.info("Initializing the application successfully.")
    val sparkConf = new SparkConf().setAppName(applicationName)
    try {
      setEnvProperties()
      val applicationConfiguration = getYamlConfiguration(configPath)
      setSparkProperties(sparkConf, applicationConfiguration)
      setElasticProperties(sparkConf, applicationConfiguration)
      val sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport.getOrCreate
      sparkSession
    } catch {
      case e: Exception =>
        throw new RuntimeException("Error in initializing spark session " + e.getCause)
    }
  }

  /*
  Application Level Setup
   */
  /**
   * Set up configuration files for each app code in the application
   * @param args
   * @return inputConfig
   */
  def setAppConfig(args: Array[String]): Map[String, Any] = {
    if (args.length < 1) {
      appLogger.error("Wrong number of arguments. Please provide app_code.yml location.")
      System.exit(1)
    }

    // build source specific config
    val appCode = StringUtils.substringsBetween(args(0), "app_", ".yml")(0)

    // for the 2nd parameter, they are cob or cob,para_1,para_2,para_3
    val cob = if (args.length == 2) args(1).split(",")(0) else AppDefaultConfig.DEFAULT_APP_DATE
    val otherPara = if (args.length == 2 && args(1).contains(",")) args(1) else ""

    // set all commandline arguments to properties in order to substitute app_code.yml
    System.setProperty("cob", cob)
    System.setProperty("other_para", otherPara)
    var n = 0;
    for (para <- otherPara.split(",")) {
      System.setProperty(s"para_${n}", para)
      n = n + 1
    }

    if (System.getProperty("FETCH_SRC_LOC", "false") == "true") {
      // here we fetch data from both url
      val dfSvcRUL =
        if (System.getProperty("FETCH_RUN_ID", "false") == "true")
          System.getProperty("DF_BATCH_LATEST_URL", "")
        else
          System.getProperty("DF_BATCH_LATEST_URL", "").replace("latest", cob)

      getRunIdAndSourcePathAsSysProps(appCode, dfSvcRUL, System.getProperty("DF_SERVICE_SPOOF", "false"))
    } else {
      if (System.getProperty("DE_TREE_OUTPUT") == null)
        System.setProperty("DE_TREE_OUTPUT", AppDefaultConfig.DEFAULT_CONF_TREE_OUTPUT)
    }

    val appName = appCode + " processing"
    val output = System.getProperty("DE_OUTPUT_ROOT_PATH", AppDefaultConfig.DEFAULT_CONF_STAGE_OUTPUT_ROOT_PATH) + s"/${appCode}"
    val outputLog = System.getProperty("DE_LOG_ROOT_PATH", AppDefaultConfig.DEFAULT_CONF_STAGE_LOG_ROOT_PATH) + s"/${appCode}"
    val inputConfig = getYamlConfiguration(args(0)) + (
      "app_code" -> appCode, "app_name" -> appName, "output" -> output, "output_log" -> outputLog,
      "cob" -> cob, "etl_start_time" -> LocalDateTime.now(), "dry_run" -> System.getProperty("DRY_RUN", "false")
    )
    inputConfig
  }

  /**
   * Wrapper for running a application/pipeline job
   * @param args
   * @param sparkSession
   * @return
   */
  def setAppRun(args: Array[String], sparkSession: SparkSession) = {
    val inputConfig = setAppConfig(args)
    if (inputConfig.getOrElse("job_disabled", "false").toString.equalsIgnoreCase("false")) {
      val transform = new TransformationStep with ProcessInterceptor
      transform.process(ProcessContext(inputConfig, sparkSession))
    }
  }

  def getRestContent(hostName: String, port: Integer, index: String, user: String, pass: String): Map[String, Any] = {
    val mapper = new ObjectMapper()
    val url = s"${index}/_count"
    val credentialsProvider = new BasicCredentialsProvider
    credentialsProvider.setCredentials(new AuthScope(hostName, port), new UsernamePasswordCredentials(user, pass))
    val authCache = new BasicAuthCache
    val basicAuth = new BasicScheme
    val host = new HttpHost(hostName, port)
    authCache.put(host, basicAuth)
    val context = HttpClientContext.create
    context.setCredentialsProvider(credentialsProvider)
    context.setAuthCache(authCache)
    val httpClient = HttpClientBuilder.create.build
    val httpGet = new HttpGet(url)

    Try(httpClient.execute(host, httpGet, context)) match {
      case Success(response) => {
        val m = mapper.readValue(response.getEntity.getContent, classOf[java.util.Map[String, String]])
        m.asScala
      }
      case Failure(e) => Map.empty
    }
  }

  def getEsCount(sparkSession: SparkSession, index: String): Int = {
    val nodes = sparkSession.sparkContext.getConf.get("es.nodes")
    val port = sparkSession.sparkContext.getConf.get("es.port").toInt
    val user = sparkSession.sparkContext.getConf.get("es.net.http.auth.user")
    val host = nodes.split(",")(0).split(":")(0)
    val pass = sparkSession.sparkContext.getConf.get("es.net.http.auth.pass")

    val content = getRestContent(host, port, index, user, pass)
    content.get("count").map(c => c.toString.toInt).getOrElse(-1) // get count from json response
  }

  def getRunIdAndSourcePathAsSysProps(appCode: String, url: String, fakeResponse: String = "false") = {
    val resJsonObj = dfUrlClient(url, fakeResponse)
    val runId = resJsonObj.get("runId").getAsString
    val loc = resJsonObj.get("location").getAsString
    val locWithoutRunId = loc.substring(0, loc.lastIndexOf("/")) // this is root path used for DE_TREE_OUTPUT

    println(s"called data service url = ${url}")

    val best1 = resJsonObj.get("level1BestRefPath").getAsString
    val best1a = resJsonObj.get("level1aBestRefPath").getAsString
    val best1b = resJsonObj.get("level1bBestRefPath").getAsString

    // add best of fields to the properties
    System.setProperty("src-best1", best1)
    System.setProperty("src-best1a", best1a)
    System.setProperty("src-best1b", best1b)

    println(s"added sys.prop src-best1 = ${best1}")
    println(s"added sys.prop src-best1a = ${best1a}")
    println(s"added sys.prop src-best1b = ${best1b}")

    val srcJsonArray = resJsonObj.get("sources").getAsJsonArray

    for (i <- 0 until srcJsonArray.size) {
      val propName = "src-" + srcJsonArray.get(i).getAsJsonObject.get("asset").getAsString
      val propValue = srcJsonArray.get(i).getAsJsonObject.get("path").getAsString
      System.setProperty(propName, propValue)
      println(s"added sys.prop ${propName}=${propValue}")
    }
    System.setProperty("DE_TREE_OUTPUT", locWithoutRunId)
    println(s"added sys.prop DE_TREE_OUTPUT=${locWithoutRunId}")
  }

  // rest client
  def curlClient(callType: String, host: String, port: Integer, url: String, body: String, user: String, pass: String): Map[String, Any] = {
    import scala.sys.process._
    val command = if (body.isEmpty)
      Seq("bath", "-c", s"""curl -k -u ${user}:${pass} -X ${callType} ${host}:${port}/${url} -H 'Content-Type:application/json'""")
    else
      Seq("bath", "-c", s"""curl -k -u ${user}:${pass} -X ${callType} ${host}:${port}/${url} -H 'Content-Type:application/json'-d'${body}""")

    Try(command.!!) match {
      case Success(response) => Map("status" -> "success", "cmd" -> command.toString.replaceAll(s"$pass", "****"), "cmd_output" -> response)
      case Failure(e) => Map("status" -> "failed", "cmd" -> command.toString.replaceAll(s"$pass", "****"), "cmd_output" -> ExceptionUtils.getStackTrace(e))
    }
  }

  def httpClient(callType: String, host: String, port: Integer, url: String, body: String, user: String, pass: String): Map[String, Any] = {
    val mapper = new ObjectMapper()
    val credentialsProvider = new BasicCredentialsProvider
    credentialsProvider.setCredentials(new AuthScope(host, port), new UsernamePasswordCredentials(user, pass))
    val authCache = new BasicAuthCache
    val basicAuth = new BasicScheme
    val hostName = new HttpHost(host, port)
    authCache.put(hostName, basicAuth)
    val context = HttpClientContext.create
    context.setCredentialsProvider(credentialsProvider)
    context.setAuthCache(authCache)
    val httpClient = HttpClientBuilder.create.build

    val stringEntity = new StringEntity(body, ContentType.APPLICATION_JSON)
    val httpCall = if (callType.toLowerCase == "put") {
      val put = new HttpPut(url)
      put.setEntity(stringEntity)
      put
    } else if (callType.toLowerCase == "get") {
      new HttpGet(url)
    } else if (callType.toLowerCase == "post") {
      val post = new HttpPost(url)
      post.setEntity(stringEntity)
      post
    } else new HttpDelete(url)

    Try(httpClient.execute(hostName, httpCall, context)) match {
      case Success(response) => {
        val m = mapper.readValue(response.getEntity.getContent, classOf[java.util.Map[String, String]])
        m.asScala
      }
      case Failure(e) => Map("failure" -> ExceptionUtils.getStackTrace(e))
    }
  }

  def dfUrlClient(urlString: String, fakeResponse: String = "false"): JsonObject = {
    val errorObj = new JsonParser().parse(AppDefaultConfig.DF_DUMMY_RESPONSE).getAsJsonObject
    val url = new URL(urlString)
    val host = new HttpHost(url.getHost, url.getPort)
    val context = HttpClientContext.create
    val httpCLient = HttpClientBuilder.create.build
    val httpCall = new HttpGet(url.getPath)

    if (fakeResponse == "true") {
      errorObj.addProperty("info", "you are using fake response")
      errorObj
    } else {
      Try(httpCLient.execute(host, httpCall, context)) match {
        case Success(response) => {
          val res = EntityUtils.toString(response.getEntity)
          new JsonParser().parse(res).getAsJsonObject
        }
        case Failure(exception) => {
          errorObj.addProperty("error", exception.getMessage)
          errorObj.addProperty("info", "data service url is down and uses dummy data.")
          errorObj
        }
      }
    }
  }

}
