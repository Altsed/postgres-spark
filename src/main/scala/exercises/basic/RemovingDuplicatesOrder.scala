package exercises.basic

import connectors.SparkConnector
import org.apache.spark.sql.functions.col
import servise.postgres.GetDataFramePostresService

/**
 *Question
   How can you produce an ordered list of the first 10 surnames in the members table? The list must not contain duplicates.
   https://pgexercises.com/questions/basic/unique.html
 */

object RemovingDuplicatesOrder extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val membersDf = GetDataFramePostresService.getDataFrame(spark, "cd.members")


  membersDf
    .select(col("surname"))
    .dropDuplicates()
    .orderBy(col("surname").asc)
    .show(100, truncate = false)
}
