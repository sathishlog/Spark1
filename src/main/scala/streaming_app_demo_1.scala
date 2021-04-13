package com.datamaking.ctv

// --class com.datamaking.ctv.streaming_app_demo

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.functions.from_json

object streaming_app_demo_1 {
  def main(args: Array[String]): Unit = {

    println("Spark Structured Streaming with Kafka Demo Application Started ...")

    val KAFKA_TOPIC_NAME_CONS = "testtopic"
    val KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

    //System.setProperty("HADOOP_USER_NAME","hadoop")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Structured Streaming with Kafka Demo")
      .config("spark.driver.host","localhost")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Stream from Kafka
    val transaction_detail_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
      .option("subscribe", KAFKA_TOPIC_NAME_CONS)
      .option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", "1")
      .load()

    println("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()
    


    val transaction_detail_df1 = transaction_detail_df.selectExpr("CAST(value AS STRING) as Value")

     val transaction_detail_schema = StructType(Array(
           StructField("Name", StringType),
           StructField("Age", StringType)

         ))

         val transaction_detail_df2 = transaction_detail_df1
           .select(from_json(col("value"), transaction_detail_schema).as("transaction_detail"))

    val transaction_detail_df3 = transaction_detail_df2.select("transaction_detail.*")

    //val   transaction_detail_df3 = transaction_detail_df2.select(to_json())






    val trans_detail_write_stream = transaction_detail_df3
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("append")
      //.option("truncate","false")
      .format("console")
      .option("path","/home/sathish/struct_strm")
      .option("checkpointLocation", "/home/sathish/struct_strm/chck")
      .start()

    //transaction_detail_df2.repartition(1).write.mode("append").json("/strcut_strm/")

    trans_detail_write_stream.awaitTermination()







    println("Spark Structured Streaming with Kafka Demo Application Completed.")
  }
}
