package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.functions._
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
 * Produce a list of the total number of slots booked per facility.
 * For now, just produce an output table consisting of facility id and slots, sorted by facility id.
 *https://pgexercises.com/questions/aggregates/fachours.html
 */

object ListTotalSlots extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")

  import spark.implicits._
  val count = bookingsDf
    .groupBy($"facid")
    .agg(sum($"slots").as("Total slots"))
//    .sum("slots").as("Total_slots") Not working
    .orderBy($"Total slots".desc)
    .show(false)


}
