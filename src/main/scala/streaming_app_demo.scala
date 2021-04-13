package com.datamaking.ctv

// --class com.datamaking.ctv.streaming_app_demo

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object streaming_app_demo {
  def main(args: Array[String]): Unit = {

    println("Spark Structured Streaming with Kafka Demo Application Started ...")

    val KAFKA_TOPIC_NAME_CONS = "testtopic"
    val KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

    //System.setProperty("HADOOP_USER_NAME","hadoop")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Structured Streaming with Kafka Demo")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Stream from Kafka
    val transaction_detail_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
      .option("subscribe", KAFKA_TOPIC_NAME_CONS)
      .option("startingOffsets", "earliest")
      .load()

    println("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()

    val transaction_detail_df1 = transaction_detail_df.selectExpr("CAST(value AS STRING)")

    // Define a schema for the transaction_detail data
    /*val transaction_detail_schema = StructType(Array(
      StructField("transaction_id", StringType),
      StructField("transaction_card_type", StringType),
      StructField("transaction_amount", StringType),
      StructField("transaction_datetime", StringType)
    ))

    val transaction_detail_df2 = transaction_detail_df1
      .select(from_json(col("value"), transaction_detail_schema)
        .as("transaction_detail"), col("timestamp"))

    val transaction_detail_df3 = transaction_detail_df2.select("transaction_detail.*", "timestamp")

    // Simple aggregate - find total_transaction_amount by grouping transaction_card_type
    val transaction_detail_df4 = transaction_detail_df3.groupBy("transaction_card_type")
      .agg(sum(col("transaction_amount")).as("total_transaction_amount"))

    println("Printing Schema of transaction_detail_df4: ")
    transaction_detail_df4.printSchema()

    val transaction_detail_df5 = transaction_detail_df4.withColumn("key", lit(100))
      .withColumn("value", concat(lit("{'transaction_card_type': '"),
        col("transaction_card_type"), lit("', 'total_transaction_amount: '"),
        col("total_transaction_amount").cast("string"), lit("'}")))

    println("Printing Schema of transaction_detail_df5: ")
    transaction_detail_df5.printSchema()*/

    // Write final result into console for debugging purpose
    val trans_detail_write_stream = transaction_detail_df1
      .writeStream
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("append")
      //.option("truncate","false")
      .format("console")
      .start()
    trans_detail_write_stream.awaitTermination()

    // Write final result in the Kafka topic as key, value
    /*val trans_detail_write_stream_1 = transaction_detail_df5
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
      .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS)
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .outputMode("update")
      .option("checkpointLocation", "/")
      .start()*/

    println("Spark Structured Streaming with Kafka Demo Application Completed.")
  }
}
