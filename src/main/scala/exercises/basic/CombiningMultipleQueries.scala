package exercises.basic

import connectors.SparkConnector
import servise.postgres.GetDataFramePostgresService

/**
 *Question
  You, for some reason, want a combined list of all surnames and all facility names. Yes, this is a contrived example :-).
  Produce that list!
  https://pgexercises.com/questions/basic/union.html
 */

object CombiningMultipleQueries extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val membersDf = GetDataFramePostgresService.getDataFrame(spark, "cd.members")
  val facilitiesDf = GetDataFramePostgresService.getDataFrame(spark, "cd.facilities")

  import spark.implicits._
  membersDf
    .select($"surname")
    .union(facilitiesDf.select($"name"))
    .show(100, truncate = false)
}
