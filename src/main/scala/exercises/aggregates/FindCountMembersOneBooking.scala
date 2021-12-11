package exercises.aggregates

import connectors.SparkConnector
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
  Find the total number of members (including guests) who have made at least one booking.
 *https://pgexercises.com/questions/aggregates/members1.html
 */

object FindCountMembersOneBooking extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")

  import spark.implicits._
  val count = bookingsDf
    .select($"memid")
    .distinct().count()

  println(s"memid count: $count")
}
