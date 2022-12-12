package exercises.timestamp

import connectors.SparkConnector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import servise.postgres.GetDataFramePostgresService.getDataFrame


/**
 * Question
For each month of the year in 2012, output the number of days in that month.
Format the output as an integer column containing the month of the year, and a second column containing an interval data type.
 *
 *
 *https://pgexercises.com/questions/date/daysinmonth.html */

object GenerateDayInMonths2012 extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")


  import spark.implicits._
  val windowMaxDate = Window.orderBy($"Date".desc)
  val windowMinDate = Window.orderBy($"Date")

  val dates = Seq("01-01-2012","31-12-2012").toDF("Date")
  val year2012 =
    dates
      .withColumn("Date", to_date($"Date","dd-MM-yyyy"))
      .withColumn("max_date", functions.max($"Date").over(windowMaxDate))
      .withColumn("min_date", functions.min($"Date").over(windowMinDate))
      .withColumn("sequence", sequence($"max_date", $"min_date"))
      .withColumn("exploded", explode($"sequence"))
      .select($"exploded")
  val days2012 = year2012
    .withColumn("days", dayofmonth(last_day($"exploded")))
    .groupBy(month($"exploded")).max("days")
    .show(false)


}
