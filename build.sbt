name := "spar-structured-streaming-demo"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.1.0"