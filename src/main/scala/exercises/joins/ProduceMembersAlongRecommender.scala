package exercises.joins

import connectors.SparkConnector
import servise.postgres.GetDataFramePostgresService

object ProduceMembersAlongRecommender extends App  {
  /**
   *Question
   How can you output a list of all members, including the individual who recommended them (if any)? Ensure that results are ordered by (surname, firstname).
   https://pgexercises.com/questions/joins/self2.html
   */
  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
  val membersDf = GetDataFramePostgresService.getDataFrame(spark, "cd.members")

  import spark.implicits._
  val recommendedbyDf = membersDf
    .select($"firstname".as("recommender_firstname"), $"surname".as("recommender_surname"), $"memid")

  val joinedDf = membersDf
    .select($"firstname", $"surname", $"recommendedby")
    .join(recommendedbyDf, membersDf("recommendedby") === recommendedbyDf("memid"), "left_outer")
    .drop($"memid")
    .orderBy($"surname", $"firstname")

  joinedDf.show(false)

}
