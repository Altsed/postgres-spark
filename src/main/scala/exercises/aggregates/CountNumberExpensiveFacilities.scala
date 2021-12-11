package exercises.aggregates

import connectors.SparkConnector
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
 * Produce a count of the number of facilities that have a cost to guests of 10 or more.
 *https://pgexercises.com/questions/aggregates/count2.html
 */

object CountNumberExpensiveFacilities extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")

  import spark.implicits._
  val count = facilitiesDf
    .filter($"guestcost" > 10)
    .count()

  println(count)
}
