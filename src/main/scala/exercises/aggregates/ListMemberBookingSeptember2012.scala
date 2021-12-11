package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number, to_date}
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 * Question
  Produce a list of each member name, id, and their first booking after September 1st 2012. Order by member ID.

 * https://pgexercises.com/questions/aggregates/nbooking.html
 */

object ListMemberBookingSeptember2012 extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")

  import spark.implicits._

  val windowSpec = Window.partitionBy($"memid").orderBy($"starttime")

  val count = bookingsDf
    .select($"memid", $"starttime")
    .where(to_date($"starttime") > "2012-09-01")
    .withColumn("first_booking", row_number().over(windowSpec))
    .where($"first_booking" === 1)
    .join(membersDf.select($"firstname", $"surname", $"memid"), Seq("memid"))
    .orderBy($"memid")
    .show(false)

}
