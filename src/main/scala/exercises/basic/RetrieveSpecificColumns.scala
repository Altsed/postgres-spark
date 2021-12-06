package exercises.basic

import connectors.SparkConnector
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
  Question
    - You want to print out a list of all of the facilities and their cost to members.
     How would you retrieve a list of only facility names and costs?
    https://pgexercises.com/questions/basic/selectspecific.html
 */

object RetrieveSpecificColumns extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")

  import spark.implicits._
  facilitiesDf
    .select($"name", $"membercost")
    .show(100, truncate = false)
}
