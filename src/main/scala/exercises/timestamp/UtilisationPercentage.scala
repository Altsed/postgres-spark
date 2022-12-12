package exercises.timestamp

import connectors.SparkConnector
import org.apache.spark.sql.functions.{last_day, to_timestamp}
import servise.postgres.GetDataFramePostgresService.getDataFrame


/**
 * Question
  Work out the utilisation percentage for each facility by month, sorted by name and month, rounded to 1 decimal place.
  Opening time is 8am, closing time is 8.30pm. You can treat every month as a full month, regardless of if there were some dates the club was not open.

 *https://pgexercises.com/questions/date/utilisationpermonth.html */

object UtilisationPercentage extends App {

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
