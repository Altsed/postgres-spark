package servise.postgres

import connectors.PostgresSqlConnector.{jdbcPostgresql, password, postgresqlDriver, user}
import org.apache.spark.sql.{DataFrame, SparkSession}

object GetDataFramePostgresService {


  def getDataFrame(spark: SparkSession, tableName: String): DataFrame = spark.read
    .format("jdbc")
    .option("driver", postgresqlDriver)
    .option("url", jdbcPostgresql)
    .option("dbtable", tableName)
    .option("user", user)
    .option("password", password)
    .load()

}
