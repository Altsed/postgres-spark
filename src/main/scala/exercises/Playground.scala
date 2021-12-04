package exercises

import org.apache.spark.sql.SparkSession

object Playground extends App {

  /*val spark = ExerciseUtils.getSparkSession("Postgres Spark Playground App")

  var df2 = spark.read.parquet("D:/temp/ITRACKQ_GL_TRNS_TIGQ_20211114-094534421.parquet.snappy")
  df2.show(false)*/

  val spark = SparkSession.builder()
    .appName("Postgres Spark Playground App")
    .config("spark.master", "spark://192.168.1.38:7077")
    .getOrCreate()

  import spark.implicits._
  val columns = Seq("language","users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

  data.toDF(columns:_*).show()

}
