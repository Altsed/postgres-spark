package modify_data

import connectors.PostgresSqlConnector.{jdbcPostgresql, password, postgresqlDriver, user}
import connectors.SparkConnector
import org.apache.spark.sql.SaveMode

object InsertRow extends App  {
  /**
   *Question
   Insert to the table
   https://pgexercises.com/questions/updates/insert.html
   */
  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")

  val facility: Facility = Facility(9, "Spa", 20, 30, 100000, 800)

  val facilities: List[Facility] = List(facility)

  import spark.implicits._
  facilities.toDF
    .write
    .format("jdbc")
    .option("driver", postgresqlDriver)
    .option("url", jdbcPostgresql)
    .option("dbtable", "cd.facilities")
    .option("user", user)
    .option("password", password)
    .mode(SaveMode.Append)
    .save()
}

case class Facility(facid: Int, name: String, membercost: Int, guestcost: Int, initialoutlay: BigInt, monthlymaintenance: BigInt)
