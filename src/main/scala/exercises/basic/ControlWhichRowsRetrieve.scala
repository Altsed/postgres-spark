package exercises.basic

import exercises.ExerciseUtils
import org.apache.spark.sql.functions.col

/**
 *Question
   How can you produce a list of facilities that charge a fee to members?
   https://pgexercises.com/questions/basic/where.html
 */

object ControlWhichRowsRetrieve extends App {

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
    .filter(col("membercost") > 0)
    .show(100, false)
}
