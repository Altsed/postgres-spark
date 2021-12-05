package exercises.basic

import connectors.SparkConnector
import org.apache.spark.sql.functions.{col, lit, when}
import servise.postgres.GetDataFramePostresService.getDataFrame

/**
 *Question
   How can you produce a list of facilities, with each labelled as 'cheap' or 'expensive' depending on if their monthly
   maintenance cost is more than $100? Return the name and monthly maintenance of the facilities in question.
   https://pgexercises.com/questions/basic/classify.html
 */

object ClassifyResults extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")

  facilitiesDf
    .select(col("name"), col("monthlymaintenance"))
    .withColumn("cost", when(col("monthlymaintenance") > 100, lit("expensive")).otherwise(lit("cheap")))
    .show(100, truncate = false)
}
