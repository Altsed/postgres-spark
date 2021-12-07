package modify_data

import connectors.SparkConnector
import servise.postgres.GetDataFramePostgresService.getDataFrame

import java.sql.DriverManager

object DeleteRow extends App  {
  /**
   *Question
   Insert to the table
   https://pgexercises.com/questions/updates/insert.html
   */
  val spark = SparkConnector.getLocalSparkSession("Spark Basic Sql Practice")

  val facility: Facility = Facility(9, "Spa", 20, 30, 100000, 800)
  val facilitiesDf = getDataFrame(spark, "cd.facilities")

  facilitiesDf.foreachPartition(iter => {

    val connection = DriverManager.getConnection("jdbc:postgresql://192.168.1.38:5432/exercises","spark", "spark")
    val statement = connection.prepareStatement("delete from cd.facilities where facid = ?")
    iter.foreach(row => {
      val facid = row.getInt(0)
      statement.setInt(1, facid)
      statement.execute()
    })
    connection.close()
  } )


}

