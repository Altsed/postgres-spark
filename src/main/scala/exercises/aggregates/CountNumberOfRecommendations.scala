package exercises.aggregates

import connectors.SparkConnector
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
 * Produce a count of the number of recommendations each member has made. Order by member ID.
 *https://pgexercises.com/questions/aggregates/count3.html
 */

object CountNumberOfRecommendations extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val membersDf = getDataFrame(spark, "cd.members")

  import spark.implicits._
  val count = membersDf
    .groupBy($"recommendedby")
    .count()
    .orderBy($"recommendedby".asc_nulls_last)
    .show(false)


}
