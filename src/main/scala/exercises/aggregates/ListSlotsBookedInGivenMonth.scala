package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.to_date
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
 Produce a list of the total number of slots booked per facility in the month of September 2012.
 Produce an output table consisting of facility id and slots, sorted by the number of slots.
 *https://pgexercises.com/questions/aggregates/fachoursbymonth.html
 */

object ListSlotsBookedInGivenMonth extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")

  import spark.implicits._
  val count = bookingsDf
    .select($"facid", $"slots", to_date($"starttime"))
    .where($"starttime".geq("2012-09-01") && $"starttime".lt("2012-10-01"))
    .groupBy($"facid")
    .agg(functions.sum($"slots").as("Total slots"))
//    .sum("slots").as("Total_slots") Not working
    .orderBy($"Total slots".desc)
    .show(false)


}
