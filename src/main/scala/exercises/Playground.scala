package exercises

import connectors.SparkConnector

object Playground extends App {

  val spark =  SparkConnector.getSparkSession("Spark Basic Sql Practice")

  import spark.implicits._
  val columns = Seq("language","users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

  data.toDF(columns:_*).show()

}
