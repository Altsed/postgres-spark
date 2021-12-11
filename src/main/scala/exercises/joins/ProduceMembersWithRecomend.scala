package exercises.joins

import connectors.SparkConnector
import servise.postgres.GetDataFramePostgresService

object ProduceMembersWithRecomend extends App  {
  /**
   *Question
   How can you output a list of all members who have recommended another member?
   Ensure that there are no duplicates in the list, and that results are ordered by (surname, firstname).
   https://pgexercises.com/questions/joins/self.html
   */
  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val membersDf = GetDataFramePostgresService.getDataFrame(spark, "cd.members")

  import spark.implicits._
  val recommendedbyDf = membersDf
    .select($"recommendedby").distinct()

  val joinedDf = membersDf
    .select($"firstname", $"surname", $"recommendedby")
    .join(recommendedbyDf, Seq("recommendedby"))
    .drop($"recommendedby")
    .orderBy($"firstname", $"surname")

  joinedDf.show(false)

}
