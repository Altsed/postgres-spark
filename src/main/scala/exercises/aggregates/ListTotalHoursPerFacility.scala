package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.format_number
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 * Question
  Produce a list of the total number of hours booked per facility, remembering that a slot lasts half an hour.
  The output table should consist of the facility id, name, and hours booked, sorted by facility id.
  Try formatting the hours to two decimal places.
 *
 * https://pgexercises.com/questions/aggregates/fachours3.html
 */

object ListTotalHoursPerFacility extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")

  import spark.implicits._
  val count = bookingsDf
    .select($"facid", $"slots")
    .withColumn("hours",  format_number($"slots" / 2, 2) ) // how to format number
    .groupBy($"facid")
//    .sum("hours").as("Total hours") // impossible to change column name
    .agg(functions.sum($"hours").as("Total hours"))
    .join(facilitiesDf.select($"name", $"facid"), Seq("facid"))
    .orderBy($"facid")


    .show(false)

}
