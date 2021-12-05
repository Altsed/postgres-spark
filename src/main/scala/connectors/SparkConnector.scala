package connectors

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkConnector {

  def getSparkSession(appName: String): SparkSession = {

    val conf = new SparkConf
    conf.set("spark.master", "spark://192.168.1.38:7077")
    conf.set("spark.driver.host", "192.168.1.194")
    //    conf.set("spark.submit.deployMode", "cluster")
    conf.set("spark.driver.bindAddress", "0.0.0.0")
    conf.set("spark.blockManager.port", "20002")
    conf.set("spark.driver.port ", "20001")
    //    conf.set("spark.executor.host", "192.168.16.3")
    //    conf.set("spark.dynamicAllocation.enabled", "false")
    conf.set("spark.app.name", appName)

    SparkSession.builder.config(conf = conf).getOrCreate()
  }

  def getLocalSparkSession(appName: String): SparkSession = {

    val conf = new SparkConf
    conf.set("spark.master", "local")
    conf.set("spark.app.name", appName)

    SparkSession.builder.config(conf = conf).getOrCreate()
  }


}
