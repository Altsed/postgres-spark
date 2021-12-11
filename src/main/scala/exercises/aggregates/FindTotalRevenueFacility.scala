package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.when
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
  Produce a list of facilities along with their total revenue. The output table should consist of facility name and revenue,
  sorted by revenue. Remember that there's a different cost for guests and members!
 * https://pgexercises.com/questions/aggregates/facrev.html
 */

object FindTotalRevenueFacility extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")

  import spark.implicits._
  val count = bookingsDf
    .select($"facid", $"slots", $"memid")
    .join(facilitiesDf.select($"guestcost",$"membercost", $"facid", $"name"), Seq("facid"))
    .withColumn("revenue", when($"memid" === 0, $"guestcost" * $"slots" ).otherwise($"membercost" * $"slots"))
    .groupBy($"name")
    .agg(functions.sum($"revenue").as("revenue"))
    .orderBy($"revenue")
    .show(false)

}
