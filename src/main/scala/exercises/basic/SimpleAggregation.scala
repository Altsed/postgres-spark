package exercises.basic

import connectors.SparkConnector
import org.apache.spark.sql.functions.max
import servise.postgres.GetDataFramePostresService

/**
 *Question
  You'd like to get the signup date of your last member. How can you retrieve this information?
  https://pgexercises.com/questions/basic/agg.html
 */

object SimpleAggregation extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val membersDf = GetDataFramePostresService.getDataFrame(spark, "cd.members")

  membersDf
    .agg(max("joindate"))
    .show(100, truncate = false)
}
