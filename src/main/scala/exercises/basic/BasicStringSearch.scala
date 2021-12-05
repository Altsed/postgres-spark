package exercises.basic

import connectors.SparkConnector
import org.apache.spark.sql.functions.col
import servise.postgres.GetDataFramePostresService.getDataFrame

/**
 *Question
    How can you produce a list of all facilities with the word 'Tennis' in their name?
   https://pgexercises.com/questions/basic/where3.html
 */

object BasicStringSearch extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")

  facilitiesDf
    .filter(col("name").contains("Tennis"))
    .show(100, truncate = false)
}
