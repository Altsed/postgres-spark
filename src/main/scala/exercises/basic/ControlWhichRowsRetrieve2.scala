package exercises.basic

import connectors.SparkConnector
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
 * How can you produce a list of facilities that charge a fee to members, and that fee is less than 1/50th of the monthly maintenance cost?
 * Return the facid, facility name, member cost, and monthly maintenance of the facilities in question.
 * https://pgexercises.com/questions/basic/where2.html
 */

object ControlWhichRowsRetrieve2 extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")

  import spark.implicits._
  facilitiesDf
    .select($"name", $"membercost", $"monthlymaintenance")
    .filter($"membercost" > 0)
    .filter($"membercost" < $"monthlymaintenance" / 50)
    .show(100, truncate = false)
}
