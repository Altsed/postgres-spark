package exercises.joins

import connectors.SparkConnector
import servise.postgres.GetDataFramePostgresService

object RetriveStartTime extends App  {
  /**
   *Question
   How can you produce a list of the start times for bookings by members named 'David Farrell'?
   https://pgexercises.com/questions/joins/simplejoin.html
   */
  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val membersDf = GetDataFramePostgresService.getDataFrame(spark, "cd.members")
  val bookingsDf = GetDataFramePostgresService.getDataFrame(spark, "cd.bookings")

  import spark.implicits._
  val selectedMembersDf = membersDf
    .select($"firstname", $"surname", $"memid")
    .where($"firstname" === "David" && $"surname" === "Farrell")


  val joinedDf = selectedMembersDf
    .join(bookingsDf, selectedMembersDf("memid") === bookingsDf("memid"))

  val joinedDf2 = selectedMembersDf
    .join(bookingsDf, Seq("memid"))

  joinedDf.show(100, truncate = false)

  joinedDf2.show(false)

  joinedDf.select($"membmemid").show(100, truncate = false)

}
