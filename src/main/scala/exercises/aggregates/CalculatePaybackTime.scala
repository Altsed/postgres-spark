package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 * Question
  Based on the 3 complete months of data so far, calculate the amount of time each facility will take to repay its cost of ownership.
  Remember to take into account ongoing monthly maintenance. Output facility name and payback time in months, order by facility name.
  Don't worry about differences in month lengths, we're only looking for a rough value here!
  https://pgexercises.com/questions/aggregates/payback.html */

object CalculatePaybackTime extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")

  import spark.implicits._


  val windowSpecDate = Window.orderBy($"year".desc, $"month".desc)
  val windowSpecRevenue = Window.partitionBy($"name")


  val selectedBookings = bookingsDf.select($"facid", $"memid", $"starttime", $"slots")
    .withColumn("month", month(to_date($"starttime")))
    .withColumn("year", year(to_date($"starttime")))
    .withColumn("rank", rank().over(windowSpecDate))
    .join(facilitiesDf.select($"guestcost", $"membercost", $"facid", $"name", $"initialoutlay", $"monthlymaintenance"), Seq("facid"))
    .withColumn("revenue", when($"memid" === 0, $"guestcost"*$"slots").otherwise($"membercost"*$"slots"))
    .where($"rank" <= 3 )
//    .withColumn("sum_revenue", sum($"revenue").over(windowSpecRevenue)/3)
    .groupBy($"name", $"initialoutlay", $"monthlymaintenance")
    .agg(functions.sum($"revenue"/3).as("sum_revenue"))
    .withColumn("months", $"initialoutlay"/($"sum_revenue" - $"monthlymaintenance"))
    .select($"name", $"months")
    .orderBy($"name")
    .show(false)
}
