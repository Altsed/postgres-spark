package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.functions
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
 * Output the facility id that has the highest number of slots booked.
 * For bonus points, try a version without a LIMIT clause. This version will probably look messy!
 * https://pgexercises.com/questions/aggregates/fachours2.html
 */

object OutputFacilityIdHighestSlotsBooked extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")

  import spark.implicits._
  val count = bookingsDf
    .select($"facid", $"slots")
    .groupBy($"facid")
//    .agg(functions.sum($"slots").desc) //why ?
    .agg(functions.sum($"slots").as("Total slots")) //why ?
    .orderBy($"Total slots")
    .first()

    println(count.mkString)

}
