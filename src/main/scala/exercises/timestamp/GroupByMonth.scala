package exercises.timestamp

import connectors.SparkConnector
import org.apache.spark.sql.functions.trunc
import servise.postgres.GetDataFramePostgresService.getDataFrame


/**
 * Question
 Return a count of bookings for each month, sorted by month

 *https://pgexercises.com/questions/date/bookingspermonth.html */

object GroupByMonth extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")

  import spark.implicits._

  bookingsDf
    .withColumn("month", trunc($"starttime", "month"))
    .groupBy($"month")
    .count()
    .orderBy($"month")
    .show(false)
}
