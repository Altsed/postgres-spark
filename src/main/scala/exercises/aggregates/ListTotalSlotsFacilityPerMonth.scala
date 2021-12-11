package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{month, year}
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
 * Produce a list of the total number of slots booked per facility per month in the year of 2012.
 * Produce an output table consisting of facility id and slots, sorted by the id and month.
 *https://pgexercises.com/questions/aggregates/fachoursbymonth2.html
 */

object ListTotalSlotsFacilityPerMonth extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")

  import spark.implicits._
  val count = bookingsDf
    .select($"facid", $"slots", $"starttime")
    .where(year($"starttime") === "2012")
    .withColumn("month", month($"starttime"))
    .groupBy($"facid", $"month")
    .agg(functions.sum($"slots").as("Total slots"))
    .orderBy($"facid", $"month")
    .show(false)

}
