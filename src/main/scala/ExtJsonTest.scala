import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructType, TimestampType}

import java.io.File


object ExtJsonTest extends App {


  val spark = SparkSession
    .builder
    .appName("spark application name")
    .config("spark.master", "local")
    .getOrCreate
  import spark.implicits._

  val schema = new StructType()
    .add("Name", StringType, false)
    .add("CreateTime", StringType, false)
    .add("UpdateTime", StringType, false)
    .add("LastAccessTime", StringType, false)

  val df = spark.read
    .schema(schema)
    .option("multiline","true")
    .json("D:/projects/cox-/temp/ext_cox.json")
//    .json("D:/projects/cox-/temp/ext_kbb.json")
//    .json("D:/projects/cox-/temp/ext_cads.json")
//    .json("D:/projects/cox-/temp/manheim_ovw.json")
    .withColumn("last_access_time", from_unixtime(col("LastAccessTime")))
    .withColumn("create_time", from_unixtime(col("CreateTime")).as("CreateTime"))
    .withColumn("update_time", from_unixtime(col("UpdateTime")).as("UpdateTime"))
    .sort($"update_time".desc)
    .show(false)

  val df_pipeline_new = spark.read.parquet("D:/projects/cox-/temp/rds/metastore/SCHEMA_CONTRACTS_AUD").drop("UPDATED_TZ").drop("REV").drop("REVTYPE")
  val df_pipeline_old = spark.read.parquet("D:/projects/cox-/temp/rds/metastore_old/SCHEMA_CONTRACTS_AUD").drop("UPDATED_TZ").drop("REV").drop("REVTYPE")
//  println((df_pipeline_new.count() +" " + df_pipeline_old.count()))
  df_pipeline_new.explain("extended")
//  println("Count is " + df_pipeline_new.exceptAll(df_pipeline_old).count())
  val diff = df_pipeline_new.join(
    df_pipeline_old
      .withColumnRenamed("REV", "REV_OLD")
      .withColumnRenamed("REVTYPE", "REVTYPE_OLD")
      .withColumnRenamed("data_asset_id", "data_asset_id_OLD")
      .withColumnRenamed("catalog_def", "catalog_def_OLD")
      .withColumnRenamed("contract_def", "contract_def_OLD")
      .withColumnRenamed("augmented_def", "augmented_def_OLD")
      .withColumnRenamed("effective_date", "effective_date_OLD")
      .withColumnRenamed("created_on", "created_on_OLD")
      .withColumnRenamed("created_by", "created_by_OLD")
      .withColumnRenamed("updated_on", "updated_on_OLD")
      .withColumnRenamed("updated_by", "updated_by_OLD")
      .withColumnRenamed("header__operation", "header__operation_old"),
    Seq("id"))
//  diff.filter($"id" === 458).show(false)
//  diff.write.option("header",true).csv("D:/projects/cox-/temp/rds/schema_aud_diff.csv")
//  private val directories: Array[String] = new File("D:/projects/cox-/temp/rds/metastore_old")
//    .listFiles
//    .filter(_.isDirectory)
//    .map(_.getName)

//  directories.map(dir => {
//
//    val df_pipeline_new = spark.read.parquet(s"D:/projects/cox-/temp/rds/metastore/$dir").drop("UPDATED_TZ")
//    val df_pipeline_old = spark.read.parquet(s"D:/projects/cox-/temp/rds/metastore_old/$dir").drop("UPDATED_TZ")
//    (dir, df_pipeline_new.exceptAll(df_pipeline_old).count())
//  }).foreach(println)


}
