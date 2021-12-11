package exercises.basic

import connectors.SparkConnector
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
   How can you produce a list of facilities that charge a fee to members?
   https://pgexercises.com/questions/basic/where.html
 */

object ControlWhichRowsRetrieve extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")

  import spark.implicits._

  facilitiesDf
    .filter($"membercost" > 0)
    .show(100, truncate = false)
}
