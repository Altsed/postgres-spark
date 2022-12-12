package exercises.timestamp

import connectors.SparkConnector
import org.apache.spark.sql.functions._
import servise.postgres.GetDataFramePostgresService.getDataFrame


/**
 * Question
 * Generate a list of all the dates in October 2012
 *
 *
 *https://pgexercises.com/questions/date/series.html */

object SecondsFromTimestamps extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")

  import spark.implicits._

  val dates = Seq("2012-08-31 01:00:00").toDF("Date")

  val period = dates
    .withColumn("Date1", to_timestamp($"Date", "yyyy-MM-dd hh:mm:ss"))
    .withColumn("Date2", to_timestamp(lit("2012-09-02 00:00:00"), "yyyy-MM-dd hh:mm:ss")) //why working without format
    .withColumn ("sec_diff", datediff($"Date1", $"Date2") * 86400)
    .show



}
