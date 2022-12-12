package exercises.timestamp

import connectors.SparkConnector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{explode, sequence, to_date}
import servise.postgres.GetDataFramePostgresService.getDataFrame

import java.time.{LocalDate, LocalTime, ZoneOffset}


/**
 * Question
 * Generate a list of all the dates in October 2012
 *
 *
 *https://pgexercises.com/questions/date/series.html */

object GenerateListDatesOctober extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")


    private val unixStart: Long = LocalDate.parse("2012-07-10").toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.UTC)
    private val unixEnd: Long = LocalDate.parse("2012-08-31").toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.UTC)

  import spark.implicits._
   // val dategenDf = (unixStart to unixEnd by 86400).map(lit).map(to_timestamp).map(to_date)
  val dategenDf = (unixStart to unixEnd by 86400).toList.toDF("timestamp")

  val windowMaxDate = Window.orderBy($"Date".desc)
  val windowMinDate = Window.orderBy($"Date")

  val dates = Seq("01-10-2012","30-10-2012").toDF("Date")
  val period =
    dates
      .withColumn("Date", to_date($"Date","dd-MM-yyyy"))
      .withColumn("max_date", functions.max($"Date").over(windowMaxDate))
      .withColumn("min_date", functions.min($"Date").over(windowMinDate))
      .withColumn("sequence", sequence($"max_date", $"min_date"))
      .withColumn("exploded", explode($"sequence"))
      .select($"exploded")
      .show()



}
