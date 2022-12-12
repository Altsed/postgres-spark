package exercises.timestamp

import connectors.SparkConnector
import org.apache.spark.sql.catalyst.dsl.expressions.localDateToLiteral
import org.apache.spark.sql.functions.{datediff, lit, to_date, to_timestamp}
import servise.postgres.GetDataFramePostgresService.getDataFrame

import java.time.LocalDate
import java.time.format.DateTimeFormatter


/**
 * Question
 * Find the result of subtracting the timestamp '2012-07-30 01:00:00' from the timestamp '2012-08-31 01:00:00'

 *
 *https://pgexercises.com/questions/date/interval.html */

object SubstructTimestamp extends App {

  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val bookingsDf = getDataFrame(spark, "cd.bookings")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val membersDf = getDataFrame(spark, "cd.members")

  import spark.implicits._

  val dateDf = Seq("2012-07-30 01:00:00").toDF("Date")

  val pattern1 = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")
  val date = LocalDate.parse("2012-08-31 01:00:00", pattern1)

  val dateColumn = date.map(lit).map(to_timestamp).map(to_date)

    dateDf.select($"Date", to_date($"Date","yyyy-MM-dd hh:mm:ss").as("to_date") )
      .withColumn("substr", datediff(to_date(dateColumn.head,"yyyy-MM-dd hh:mm:ss" ), $"Date"))
      .show(false)

}
