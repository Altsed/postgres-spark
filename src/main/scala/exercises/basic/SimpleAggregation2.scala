package exercises.basic

import connectors.SparkConnector
import org.apache.spark.sql.functions.col
import servise.postgres.GetDataFramePostgresService

/**
 *Question
 * You'd like to get the first and last name of the last member(s) who signed up - not just the date. How can you do that?
 *https://pgexercises.com/questions/basic/agg2.html
 */

object SimpleAggregation2 extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val membersDf = GetDataFramePostgresService.getDataFrame(spark, "cd.members")


  membersDf
    .select(col("surname"),col("firstname"), col("joindate"))
    .orderBy(col("joindate").desc).limit(1)  // not sure that it always be last date, better use head or first
    .show(100, truncate = true)


}
