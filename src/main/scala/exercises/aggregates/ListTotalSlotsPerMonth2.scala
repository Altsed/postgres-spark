package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{month, year}
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
  Produce a list of the total number of slots booked per facility per month in the year of 2012.
  In this version, include output rows containing totals for all months per facility,
  and a total for all months for all facilities. The output table should consist of facility id, month and slots,
  sorted by the id and month.
  When calculating the aggregated values for all months and all facids, return null values in the month and facid columns.

 * https://pgexercises.com/questions/aggregates/fachoursbymonth3.html
 */

object ListTotalSlotsPerMonth2 extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")

  val window = Window.partitionBy("$facid")

  import spark.implicits._
  val count = bookingsDf
    .select($"facid", $"slots", $"memid", $"starttime")
    .where(year($"starttime") === "2012")
    .withColumn("month", month($"starttime"))
    .rollup($"facid", $"month")
    .agg(functions.sum($"slots").as("Total slots"))
    .orderBy($"facid".asc_nulls_last, $"Total slots" )


    .show(false)

}
