package exercises.basic

import exercises.ExerciseUtils
import org.apache.spark.sql.functions.col

/**
  Question
    - You want to print out a list of all of the facilities and their cost to members.
     How would you retrieve a list of only facility names and costs?
    https://pgexercises.com/questions/basic/selectspecific.html
 */

object RetrieveSpecificColumns extends App {

  val spark = ExerciseUtils.getLocalSparkSession("Spark Basic Sql Practice")

  val facilitiesDf = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://192.168.1.38:5432/exercises")
    .option("dbtable", "cd.facilities")
    .option("user", "spark")
    .option("password", "spark")
    .load()

  facilitiesDf
    .select(col("name"), col("membercost"))
    .show(100, false)
}
