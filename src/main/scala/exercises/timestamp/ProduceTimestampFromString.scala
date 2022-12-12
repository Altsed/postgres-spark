package exercises.timestamp

import connectors.SparkConnector
import org.apache.spark.sql.functions.to_date
import servise.postgres.GetDataFramePostgresService.getDataFrame


/**
 * Question
  Produce a timestamp for 1 a.m. on the 31st of August 2012.

 *https://pgexercises.com/questions/date/timestamp.html */

object ProduceTimestampFromString extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")

  import spark.implicits._

  val dateDf = Seq(("01-08-2012")).toDF("Date")

    dateDf.select($"Date", to_date(($"Date"),"MM-dd-yyyy").as("to_date"))

    .show(false)


//  timestamp '2012-07-10', '2012-08-31','1 day'

//  private val unixStart: Long = LocalDate.parse("2012-07-10").toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.UTC)
//  private val unixEnd: Long = LocalDate.parse("2012-08-31").toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.UTC)
//
//  val dategenDf = (unixStart to unixEnd by 86400).map(lit).map(to_timestamp).map(to_date)
//
//  revdataDf.withColumn("dategen", array(dategenDf:_*))
//    .show(false)


}
