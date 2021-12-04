package exercises

object Playground extends App {

  val spark = ExerciseUtils.getSparkSession("Postgres Spark Playground App")

  var df2 = spark.read.parquet("D:/temp/ITRACKQ_GL_TRNS_TIGQ_20211114-0945344.parquet.snappy")
  df2.show(false)

}
