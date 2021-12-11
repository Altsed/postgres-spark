package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 * Question
  Produce a list of member names, with each row containing the total member count.
  Order by join date, and include guest members.

 * https://pgexercises.com/questions/aggregates/countmembers.html
 */

object ProduceListMemberTotalMemberCount extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")

  import spark.implicits._

  val windowSpec = Window.orderBy($"joindate") // expensive operation !

  val count = membersDf
    .select($"firstname", $"surname", $"joindate")
    .withColumn("row_number", row_number.over(windowSpec))
    .show(false)

}
