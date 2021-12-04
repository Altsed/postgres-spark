name := "postgres-spark"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.2.0"
val postgresVersion = "42.2.2"

resolvers ++= Seq(

)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.4.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.4.1",
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion,

)