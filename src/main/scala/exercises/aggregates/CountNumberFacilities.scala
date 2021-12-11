package exercises.aggregates

import connectors.SparkConnector
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
    For our first foray into aggregates, we're going to stick to something simple.
    We want to know how many facilities exist - simply produce a total count.
    https://pgexercises.com/questions/aggregates/count.html
 */

object CountNumberFacilities extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")

  println(facilitiesDf.count())
}
