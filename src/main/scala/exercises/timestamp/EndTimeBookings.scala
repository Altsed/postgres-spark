package exercises.timestamp

import connectors.SparkConnector
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{datediff, last_day, round, trunc}
import servise.postgres.GetDataFramePostgresService.getDataFrame


/**
 * Question
Work out the utilisation percentage for each facility by month, sorted by name and month, rounded to 1 decimal place.
Opening time is 8am, closing time is 8.30pm. You can treat every month as a full month, regardless of if there were some dates the club was not open.
 *https://pgexercises.com/questions/date/endtimes.html */

object EndTimeBookings extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")


  import spark.implicits._
  bookingsDf
    .withColumn("month", trunc($"starttime", "month"))
    .groupBy($"month", $"facid")
    .agg(functions.sum($"slots").as("slots"))
    .withColumn("days_in_month", datediff(last_day($"month"), $"month"))
    .withColumn("sec_in_month", $"days_in_month"*12.5*60*60)
    .withColumn("slots_in_sec", $"slots"*30*60)
    .withColumn("percents",  round(($"slots_in_sec"*100)/$"sec_in_month", 1))
    .join(facilitiesDf.select($"name", $"facid"), Seq("facid"))
    .orderBy($"name")

    .show(false)



}
