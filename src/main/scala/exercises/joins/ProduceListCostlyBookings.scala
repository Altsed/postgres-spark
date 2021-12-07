package exercises.joins

import connectors.SparkConnector
import org.apache.spark.sql.functions._
import servise.postgres.GetDataFramePostgresService.getDataFrame

object ProduceListCostlyBookings extends App  {
  /**
   *Question
   How can you produce a list of bookings on the day of 2012-09-14 which will cost the member (or guest) more than $30?
   Remember that guests have different costs to members (the listed costs are per half-hour 'slot'),
   and the guest user is always ID 0. Include in your output the name of the facility, the name of the member formatted as a single column,
   and the cost. Order by descending cost, and do not use any subqueries.
   https://pgexercises.com/questions/joins/threejoin2.html
   */
  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")

  import spark.implicits._
  val membersDf = getDataFrame(spark, "cd.members")

  val facilitiesDf = getDataFrame(spark, "cd.facilities")

  val bookingsDf = getDataFrame(spark, "cd.bookings")
    .where(to_date($"starttime") === "2012-09-14")

  val joinedDf = membersDf
    .select(concat($"firstname", lit(" "), $"surname").as("member"), $"memid")
    .join(bookingsDf.select($"facid", $"memid", $"slots"), Seq("memid"))
    .join(facilitiesDf.select($"facid", $"membercost", $"guestcost", $"name".as("facility")), Seq("facid"))
    .withColumn("cost",
      when(
        $"memid" === 0,
        round($"guestcost" * $"slots")).otherwise(round($"membercost"* $"slots"))
    )
    .filter($"cost" > 30)
    .drop($"facid")
    .drop($"memid")
    .drop($"guestcost")
    .drop($"membercost")

  joinedDf.printSchema()
  joinedDf
    .orderBy($"cost".desc)
    .show(false)

}
