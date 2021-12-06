package exercises.joins

import connectors.SparkConnector
import org.apache.spark.sql.functions.to_date
import servise.postgres.GetDataFramePostgresService

object WorkOutStartTimes extends App  {
  /**
   *Question
   How can you produce a list of the start times for bookings for tennis courts, for the date '2012-09-21'?
   Return a list of start time and facility name pairings, ordered by the time.
   https://pgexercises.com/questions/joins/simplejoin2.html
   */
  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val facilitiesDf = GetDataFramePostgresService.getDataFrame(spark, "cd.facilities")
  val bookingsDf = GetDataFramePostgresService.getDataFrame(spark, "cd.bookings")

  import spark.implicits._
  val selectedFacilitiesDf = facilitiesDf
    .select($"name", $"facid")
    .where($"name".like("Tennis Court %"))

  val selectedBookingsDf = bookingsDf
    .select($"facid", $"starttime")
    .where(to_date($"starttime") === "2012-09-21")

  val joinedDf = selectedFacilitiesDf
    .join(selectedBookingsDf, Seq("facid"))
    .orderBy($"starttime")

  joinedDf.show(100, truncate = false)


}
