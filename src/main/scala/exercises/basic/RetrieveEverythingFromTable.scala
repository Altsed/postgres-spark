package exercises.basic

import org.apache.spark.sql.SparkSession

/**
  Question
    - How can you retrieve all the information from the cd.facilities table?
 */

object RetrieveEverythingFromTable extends App {

  val spark = SparkSession.builder()
    .appName("Spark Basic Sql Practice")
    .config("spark.master", "local[*]")
    .getOrCreate()

  val facilitiesDf = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/exercises")
    .option("dbtable", "cd.facilities")
    .option("user", "spark")
    .option("password", "spark")
    .load()

  facilitiesDf.show(100, false)
}
