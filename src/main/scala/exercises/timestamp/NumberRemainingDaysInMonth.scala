package exercises.timestamp

import connectors.SparkConnector
import org.apache.spark.sql.functions.{last_day, to_timestamp}
import servise.postgres.GetDataFramePostgresService.getDataFrame


/**
 * Question
For any given timestamp, work out the number of days remaining in the month.
The current day should count as a whole day, regardless of the time.
Use '2012-02-11 01:00:00' as an example timestamp for the purposes of making the answer.
Format the output as a single interval value.

 *https://pgexercises.com/questions/date/daysremaining.html */

object NumberRemainingDaysInMonth extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")

  import spark.implicits._

  val dateDf = Seq(("2012-02-11 01:00:00")).toDF("Date")

    dateDf
      .select($"Date", to_timestamp($"Date","yyyy-MM-dd HH:mm:ss").as("to_timestamp"))
      .withColumn("till_month_end", to_timestamp(last_day($"to_date")) - to_timestamp($"to_date"))
      .show(false)
}
