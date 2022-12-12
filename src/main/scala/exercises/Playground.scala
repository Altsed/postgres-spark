package exercises

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec

object Playground extends App {

//  val spark =  SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")
//
//    import spark.implicits._
//  val columns = Seq("language","users_count")
//  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
//
//  data.toDF(columns:_*).show()

  val spark = SparkSession
    .builder
    .appName("spark application name")
    .config("spark.master", "local")
    .config("spark.hadoop.fs.s3a.access.key", "AKIAXZNXDRKFG3ZT7BAD")
    .config("spark.hadoop.fs.s3a.secret.key", "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")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate


  val fileName = "s3a://altsed-spark-test/weather/month=9/day=1/part-00155-44bd3411-fbe4-4e16-b667-7ec0fc3ad489.c000.snappy.parquet"
  val fileName2 = "s3a://altsed-spark-test/weather/month=9/day=13/part-00200-44bd3411-fbe4-4e16-b667-7ec0fc3ad489.c000.snappy.parquet"

  val df  = spark.read.parquet(fileName, fileName2)
  df.show()

  private def getColumnsWithSpecificFormat(dataFrame: DataFrame,
                                           parsedSchema: ParsedSchema,
                                           formatNames: Seq[String]): Map[String, Set[String]] = {
    dataFrame.columns.foldLeft[Map[String, Set[String]]](Map())((result, column) => {
      parsedSchema.getJsObject.value match {
        case Some(format) if formatNames.contains(format) => result + (format -> result.get(format).foldLeft(Set(column))(_++_))
        case _=> result
      }
    })
  }



  private def getColumnsWithSpecificFormat2(dataFrame: DataFrame,
                                            parsedSchema: ParsedSchema,
                                            formatNames: Seq[String]): Map[String, Set[String]] = {
    @tailrec
    def getColumnsTailRec(columNames: Array[String], acc: Map[String, Set[String]]): Map[String, Set[String]] = {
      if (columNames.isEmpty) acc
      else getColumnsTailRec(columNames.tail, updateAcc(columNames.head, acc))
    }

    def updateAcc(columnName: String, acc: Map[String, Set[String]]): Map[String, Set[String]] = {
      val jsObject = parsedSchema.getJsObject
      if (formatNames.contains(jsObject.value.get)) acc + (jsObject.value.get -> (acc.getOrElse(jsObject.value.get, Set()) + columnName))
      else acc
    }
    getColumnsTailRec(dataFrame.columns, Map())
  }

}

class ParsedSchema() {
  def getJsObject: JsObject = new JsObject
}

class JsObject() {
  def value: Option[String] = Some("format1")
}
