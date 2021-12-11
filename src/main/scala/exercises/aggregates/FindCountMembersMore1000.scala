package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.functions
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 *Question
 * Produce a list of facilities with more than 1000 slots booked.
 * Produce an output table consisting of facility id and slots, sorted by facility id.
 * https://pgexercises.com/questions/aggregates/fachours1a.html
 */

object FindCountMembersMore1000 extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")

  import spark.implicits._
  val count = bookingsDf
    .select($"facid", $"slots")
    .groupBy($"facid")
    .agg(functions.sum($"slots").as("Total slots"))
    .filter($"Total slots" > 1000)
    .orderBy($"facid")
    .show(false)

}
