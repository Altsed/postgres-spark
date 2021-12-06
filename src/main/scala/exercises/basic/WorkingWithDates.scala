package exercises.basic

import connectors.SparkConnector
import org.apache.spark.sql.functions.col
import servise.postgres.GetDataFramePostgresService

/**
 *Question
   How can you produce a list of members who joined after the start of September 2012? Return the memid, surname, firstname,
   and joindate of the members in question.
   https://pgexercises.com/questions/basic/date.html
 */

object WorkingWithDates extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val membersDf = GetDataFramePostgresService.getDataFrame(spark, "cd.members")

  import spark.implicits._
  membersDf
    .select($"memid", $"surname", $"firstname", $"joindate")
    .filter(col("joindate") > "2012-09-01")
    .show(100, truncate = false)
}
