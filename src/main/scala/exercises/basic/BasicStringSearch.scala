package exercises.basic

import connectors.SparkConnector
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
    How can you produce a list of all facilities with the word 'Tennis' in their name?
   https://pgexercises.com/questions/basic/where3.html
 */

object BasicStringSearch extends App {

  val spark = SparkConnector.getSparkSession("Spark Basic Sql Practice")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")

  import spark.implicits._

  facilitiesDf
    .filter($"name".contains("Tennis"))
    .show(100, truncate = false)
}
