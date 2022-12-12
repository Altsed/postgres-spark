name := "postgres-spark"

version := "0.1"

scalaVersion := "2.12.12"

val sparkVersion = "3.2.0"
val postgresVersion = "42.2.2"
val awsSdkVersion = "1.11.1008"

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
  "com.amazonaws" % "aws-java-sdk-s3" % awsSdkVersion % "provided",
  "com.amazonaws" % "aws-java-sdk-bom" % awsSdkVersion % "provided",
  "com.amazonaws" % "aws-java-sdk-sts" % awsSdkVersion % "provided",
  "com.amazonaws" % "aws-java-sdk-cloudwatch" % awsSdkVersion % "provided",
  "org.apache.hadoop" % "hadoop-client" % "3.3.1",
  "org.apache.hadoop" % "hadoop-common" % "3.3.1",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.1",
  "org.apache.parquet" % "parquet-avro" % "1.12.2",
  "org.apache.avro" % "avro" % "1.11.0",
  // streaming
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  // streaming-kafka
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,
)