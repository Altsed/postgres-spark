package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{to_date, when}
import org.apache.spark.sql.types.{LongType, TimestampType}
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

  val windowSpecDateMinus15days = Window
    .orderBy($"date".cast(TimestampType).cast(LongType))
    .rangeBetween(-daysToLong(15), 0)




  val groupDf3 = selectedBookings
    .select(to_date($"starttime", "yyyy-MM-dd").as("date"), $"memid", $"facid",$"slots")
    .join(facilitiesDf.select($"facid", $"guestcost", $"membercost"), Seq("facid"))
    .withColumn("revenue", when($"memid" === 0, $"guestcost"*$"slots").otherwise($"membercost"*$"slots"))
    .withColumn("revenue_per_day", functions.sum($"revenue").over(Window.partitionBy($"date")))
//    .withColumn("avg_per_15days", functions.avg($"revenue_per_day").over(windowSpecDateMinus15days)) // diff results from agg, why?
    .groupBy($"date", $"revenue_per_day")
    .agg(functions.avg($"revenue_per_day").over(windowSpecDateMinus15days).as("avg_per_15days"))
    .filter($"date" > "2012-07-31" && $"date" < "2012-09-01")
    .orderBy($"date")

}
