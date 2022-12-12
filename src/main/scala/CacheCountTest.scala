import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CacheCountTest extends App {


  val spark = SparkSession
    .builder
    .appName("spark application name")
    .config("spark.master", "local")
    .getOrCreate
import spark.implicits._
  val sampleFile2 = getClass.getResource("/sample2.txt")
  val df = spark.read.textFile(sampleFile2.getFile).select(explode(split($"value", " "))).show(false)




}
