package exercises.joins

import connectors.SparkConnector
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.lit
import servise.postgres.GetDataFramePostgresService.getDataFrame

object ProduceListOfMembersThreeJoins extends App  {
  /**
   *Question
   How can you produce a list of all members who have used a tennis court?
   Include in your output the name of the court, and the name of the member formatted as a single column.
   Ensure no duplicate data, and order by the member name followed by the facility name.
  https://pgexercises.com/questions/joins/threejoin.html
   */
  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val membersDf = getDataFrame(spark, "cd.members")
  val facilitiesDf = getDataFrame(spark, "cd.facilities")
  val bookingsDf = getDataFrame(spark, "cd.bookings")

  import spark.implicits._
  val joinedDf = membersDf
    .select(functions.concat($"firstname", lit(" "), $"surname").as("name"), $"memid")
    .join(bookingsDf.select($"facid", $"memid"), Seq("memid"))
    .join(facilitiesDf.select($"name".as("facility"), $"facid"), Seq("facid"))
    .drop($"facid")
    .drop($"memid")
    .distinct()


  joinedDf
    .orderBy($"facility")
    .show(false)

}
