package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{avg, to_date, unix_timestamp, when}
import servise.postgres.GetDataFramePostgresService.getDataFrame


/**
 * Question
  For each day in August 2012, calculate a rolling average of total revenue over the previous 15 days.
  Output should contain date and revenue columns, sorted by the date.
  Remember to account for the possibility of a day having zero revenue.
  This one's a bit tough, so don't be afraid to check out the hint!
  *https://pgexercises.com/questions/aggregates/rollingavg.html */

object CalculateRollingAverageRevenues extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")

  import spark.implicits._

  val selectedBookings = bookingsDf.select($"facid", $"memid", $"slots", $"starttime")

  def daysToLong(quantityOfDays: Int): Int = quantityOfDays * 86400

  val windowSpecDateMinus14days = Window
    .orderBy(unix_timestamp($"date"))
    .rangeBetween(-daysToLong(14), 0)


  val revdataDf =  selectedBookings
    .select(to_date($"starttime", "yyyy-MM-dd").as("date"), $"memid", $"facid",$"slots")
    .join(facilitiesDf.select($"facid", $"guestcost", $"membercost"), Seq("facid"))
    .withColumn("revenue", when($"memid" === 0, $"guestcost"*$"slots").otherwise($"membercost"*$"slots"))
    .groupBy($"date")
    .agg(functions.sum("revenue").as("revenue"))
    .filter($"date">= "2012-07-10" && $"date" <= "2012-08-31")
    .withColumn("avg_revenue", avg($"revenue").over(windowSpecDateMinus14days))
    .filter($"date" >= "2012-08-01")
    .show(false)


//  timestamp '2012-07-10', '2012-08-31','1 day'

//  private val unixStart: Long = LocalDate.parse("2012-07-10").toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.UTC)
//  private val unixEnd: Long = LocalDate.parse("2012-08-31").toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.UTC)
//
//  val dategenDf = (unixStart to unixEnd by 86400).map(lit).map(to_timestamp).map(to_date)
//
//  revdataDf.withColumn("dategen", array(dategenDf:_*))
//    .show(false)


}
