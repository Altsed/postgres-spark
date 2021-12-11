package exercises.aggregates

import connectors.SparkConnector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{lit, ntile, when}
import servise.postgres.GetDataFramePostgresService.getDataFrame

/**
 * Question
  Classify facilities into equally sized groups of high, average, and low based on their revenue.
  Order by classification and facility name.
  https://pgexercises.com/questions/aggregates/classify.html */

object ClassifyFacilitiesByValue extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")

  import spark.implicits._

  val selectedBookings = bookingsDf.select($"facid", $"memid", $"slots")
  val windowSpecGroupBy = Window.partitionBy($"name")orderBy($"revenue".desc)
  val windowSpecTop = Window.orderBy($"revenue".desc)


  val groupDf2 = selectedBookings
    .groupBy($"memid", $"facid")
    .agg(functions.sum($"slots").as("slots_cnt"))
    .join(facilitiesDf.select($"name", $"facid", $"guestcost", $"membercost"), Seq("facid"))
    .withColumn("revenue", when($"memid" ===0,$"slots_cnt" * $"guestcost").otherwise($"slots_cnt" * $"membercost"))
    .groupBy($"name")
    .agg(functions.sum($"revenue").as("revenue"))
    .withColumn("total_revenue",
      when(ntile(3).over(windowSpecTop) ===1, lit("hight"))
        .when(ntile(3).over(windowSpecTop) ===2, lit("average"))
        .when(ntile(3).over(windowSpecTop) ===3, lit("low")))
    .drop($"revenue")
    .show(false)
}
