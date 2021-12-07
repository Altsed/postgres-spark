package exercises.joins

import connectors.SparkConnector
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import servise.postgres.GetDataFramePostgresService.getDataFrame

object ProduceListMembersNoJoins extends App  {
  /**
   *Question
   How can you output a list of all members, including the individual who recommended them (if any), without using any joins?
   Ensure that there are no duplicates in the list, and that each firstname + surname pairing is formatted as a column and ordered.
   https://pgexercises.com/questions/joins/sub.html
   */
  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val membersDf = getDataFrame(spark, "cd.members")

  import spark.implicits._
  val recommendedbyDf = membersDf
    .withColumn("recommender", concat($"firstname", lit(" "), $"surname"))
    .select($"recommender", $"memid")


  val joinedDf = membersDf
    .select($"firstname", $"surname", $"recommendedby")
    .join(recommendedbyDf, membersDf("recommendedby") === recommendedbyDf("memid"), "left_outer")
    .withColumn("name", concat($"firstname", lit(" "), $"surname"))
    .drop($"memid")
    .drop($"recommendedby")
    .drop($"firstname")
    .drop($"surname")
    .na.fill("")
    .orderBy($"name")

  val columns: Array[Column] = joinedDf.columns.sorted.map(col)

  joinedDf.select(columns:_*).show(false)


  joinedDf.show(false)

}
