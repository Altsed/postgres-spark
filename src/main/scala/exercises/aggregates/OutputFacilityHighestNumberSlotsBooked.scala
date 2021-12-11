package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.rank
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 * Question
  Output the facility id that has the highest number of slots booked.
  Ensure that in the event of a tie, all tieing results get output.
 *https://pgexercises.com/questions/aggregates/fachours4.html */

object OutputFacilityHighestNumberSlotsBooked extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")

  import spark.implicits._

  val windowSpec = Window.partitionBy($"facid").orderBy(functions.sum($"slots"))

  val count = bookingsDf
    .withColumn("number", rank().over(windowSpec)) //?
    .withColumn("sum", functions.sum($"slots"))
    .where($"number" === 1)

//    .groupBy($"facid")
//    .agg(functions.sum("slots").as("total slots"))
//    .orderBy($"total slots".desc)
//    .limit(1)

    .show(false)

}
