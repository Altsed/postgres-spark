package exercises.basic

import connectors.SparkConnector
import servise.postgres.GetDataFramePostgresService

/**
 *Question
   How can you produce an ordered list of the first 10 surnames in the members table? The list must not contain duplicates.
   https://pgexercises.com/questions/basic/unique.html
 */

object RemovingDuplicatesOrder extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val membersDf = GetDataFramePostgresService.getDataFrame(spark, "cd.members")

  import spark.implicits._
  membersDf
    .select($"surname")
    .dropDuplicates()
    .orderBy($"surname".asc)
    .show(100, truncate = false)
}
