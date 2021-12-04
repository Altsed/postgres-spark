package exercises.basic

import exercises.ExerciseUtils

/**
  Question
    - How can you retrieve all the information from the cd.facilities table?
 */

object RetrieveEverythingFromTable extends App {

  val spark = ExerciseUtils.getLocalSparkSession("Spark Basic Sql Practice")

  val facilitiesDf = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://192.168.1.38:5432/exercises")
    .option("dbtable", "cd.facilities")
    .option("user", "spark")
    .option("password", "spark")
    .load()

  facilitiesDf.show(100, false)
}
