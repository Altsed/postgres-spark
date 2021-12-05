package exercises.basic

import connectors.SparkConnector
import org.apache.spark.sql.functions.col
import servise.postgres.GetDataFramePostresService

/**
 *Question
  You, for some reason, want a combined list of all surnames and all facility names. Yes, this is a contrived example :-).
  Produce that list!
  https://pgexercises.com/questions/basic/union.html
 */

object CombiningMultipleQueries extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val membersDf = GetDataFramePostresService.getDataFrame(spark, "cd.members")
  val facilitiesDf = GetDataFramePostresService.getDataFrame(spark, "cd.facilities")

  membersDf
    .select(col("surname"))
    .union(facilitiesDf.select(col("name")))
    .show(100, truncate = false)
}
