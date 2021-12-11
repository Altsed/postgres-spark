package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{round, row_number}
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 * Question
  Produce a list of members (including guests), along with the number of hours they've booked in facilities,
  rounded to the nearest ten hours. Rank them by this rounded figure, producing output of first name,
  surname, rounded hours, rank. Sort by rank, surname, and first name.
 *https://pgexercises.com/questions/aggregates/fachours4.html */

object RankMembersByHours extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")

  import spark.implicits._

  val windowSpecMemid = Window.partitionBy($"memid").orderBy($"slots")
  val windowAggMemid = Window.partitionBy($"memid")

  val selectedBookings = bookingsDf.select($"memid", $"slots")
  val sumDf = selectedBookings.withColumn("row", row_number().over(windowSpecMemid))
    .withColumn("agg_sum", functions.sum($"slots"/2).over(windowAggMemid))
    .where($"row" === 1).select($"agg_sum", $"memid")
    .withColumn("total sum", round($"agg_sum"/10, 0)*10)
    .join(membersDf.select($"firstname", $"surname", $"memid"), Seq("memid"))
    .drop($"agg_sum")
    .drop($"memid")
    .orderBy($"total sum".desc)

    .show(false)
}
