package exercises.basic

import connectors.SparkConnector
import servise.postgres.GetDataFramePostresService.getDataFrame

/**
 * Question
 * - How can you retrieve all the information from the cd.facilities table?
 */

object RetrieveEverythingFromTable extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")


  facilitiesDf.show(100, truncate = false)
}
