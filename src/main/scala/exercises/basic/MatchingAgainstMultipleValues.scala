package exercises.basic

import connectors.SparkConnector
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
   How can you retrieve the details of facilities with ID 1 and 5? Try to do it without using the OR operator.
   https://pgexercises.com/questions/basic/where4.html
 */

object MatchingAgainstMultipleValues extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")

  import spark.implicits._
  facilitiesDf
    .filter($"facid" === 1 || $"facid" === 5)
    .show(100, truncate = false)
}
