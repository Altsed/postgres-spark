package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{rank, when}
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 * Question
  Produce a list of the top three revenue generating facilities (including ties).
  Output facility name and rank, sorted by rank and facility name.
  https://pgexercises.com/questions/aggregates/facrev3.html *  */

object FindTopThreeRevenue extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")

  import spark.implicits._

  val selectedBookings = bookingsDf.select($"facid", $"memid", $"slots")
  val windowSpecGroupBy = Window.partitionBy($"name")orderBy($"revenue".desc)
  val windowSpecTop = Window.orderBy($"revenue".desc)


  val groupDf = selectedBookings
    .groupBy($"memid", $"facid")
    .agg(functions.sum($"slots").as("slots_cnt"))
    .join(facilitiesDf.select($"name", $"facid", $"guestcost", $"membercost"), Seq("facid"))
    .withColumn("revenue", when($"memid" ===0,$"slots_cnt" * $"guestcost").otherwise($"slots_cnt" * $"membercost"))
    .withColumn("rank", rank().over(windowSpecGroupBy))
    .where($"rank" === 1)
    .withColumn("total_revenue", rank().over(windowSpecTop))
    .where($"total_revenue" <= 3)
    .drop($"facid")
    .drop($"memid")
    .drop($"slots_cnt")
    .drop($"guestcost")
    .drop($"membercost")
    .drop($"rank")
    .drop("revenue")
    .orderBy($"total_revenue".desc)
    .show(false)

  val groupDf2 = selectedBookings
    .groupBy($"memid", $"facid")
    .agg(functions.sum($"slots").as("slots_cnt"))
    .join(facilitiesDf.select($"name", $"facid", $"guestcost", $"membercost"), Seq("facid"))
    .withColumn("revenue", when($"memid" ===0,$"slots_cnt" * $"guestcost").otherwise($"slots_cnt" * $"membercost"))
    .groupBy($"name")
    .agg(functions.sum($"revenue").as("revenue"))
    .withColumn("total_revenue", rank().over(windowSpecTop))
    .where($"total_revenue" <= 3)
    .drop($"revenue")
    .show(false)
}
