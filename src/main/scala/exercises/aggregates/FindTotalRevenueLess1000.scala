package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.when
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
  Produce a list of facilities with a total revenue less than 1000.
  Produce an output table consisting of facility name and revenue, sorted by revenue.
  Remember that there's a different cost for guests and members!
 * https://pgexercises.com/questions/aggregates/facrev2.html
 */

object FindTotalRevenueLess1000 extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")

  import spark.implicits._
  val count = bookingsDf
    .select($"facid", $"slots", $"memid")
    .join(facilitiesDf.select($"name", $"facid", $"membercost", $"guestcost"), Seq("facid"))
    .withColumn("revenue", when($"memid" === 0, $"guestcost" * $"slots").otherwise($"membercost" * $"slots"))
    .groupBy($"name")
    .agg(functions.sum($"revenue").as("revenue"))
    .filter($"revenue" < 1000)
    .show(false)

}
