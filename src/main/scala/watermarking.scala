package com.datamaking.ctv

// --class com.datamaking.ctv.streaming_app_demo

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.storage.StorageLevel

object watermarking {
  def main(args: Array[String]): Unit = {

    println("Spark Structured Streaming with Kafka Demo Application Started ...")

    val KAFKA_TOPIC_NAME_CONS = "testtopic"
    val KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

    //System.setProperty("HADOOP_USER_NAME","hadoop")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Structured Streaming with Kafka Demo")
      //.config("spark.driver.host","localhost")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Stream from Kafka
    val transaction_detail_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
      .option("subscribe", KAFKA_TOPIC_NAME_CONS)
      .option("startingOffsets", "latest")
      //.option("auto.offset.reset", "latest")
      //.option("maxOffsetsPerTrigger", "10")
      .load()

    println("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()


    val transaction_detail_df1 = transaction_detail_df.selectExpr("CAST(value AS STRING) as Value")

    transaction_detail_df1.printSchema()
    val transaction_detail_schema = StructType(Array(
      StructField("Name", StringType),
      StructField("Age", StringType),
      StructField("Ts", StringType)
    ))

    val transaction_detail_df2 = transaction_detail_df1
      .select(from_json(col("value"), transaction_detail_schema).as("transaction_detail"))

    val transaction_detail_df4 = transaction_detail_df2.selectExpr("transaction_detail.*")
    //val rn_qry = Window.partitionBy("Name").orderBy("Age")
    val transaction_detail_df5 = transaction_detail_df4
                                  .withColumn("Ts",col("Ts").cast("timestamp"))


    //val transaction_detail_df6 = transaction_detail_df5.withWatermark("Ts", "50 seconds")

    /*val qry = transaction_detail_df5
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("append")
      .option("truncate","false")
      .format("console")
      //.option("path","/home/sathish/struct_strm")
      .option("checkpointLocation", "hdfs://localhost:9000/struct_strm")
      .start()*/

      /*val qry = transaction_detail_df5
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("append")
      //.option("truncate","false")
      .format("memory")
      .queryName("test")
      //.option("path","/home/sathish/struct_strm")
      //.option("checkpointLocation", "hdfs://localhost:9000/struct_strm")
      .start()

    spark.sql("select * from test").show(false)*/

    def intcal(batchDF:DataFrame , batchId: Long ): Unit ={
      batchDF.persist()
      if(!(batchDF.filter(col("Name") === "Sathish" ).rdd.isEmpty())) {
        batchDF.filter(col("Name") === "Sathish").repartition(1).write.mode("append").json("/home/sathish/struct_strm/chck/sathish")
      }
      if(!(batchDF.filter(col("Name") === "Shree" ).rdd.isEmpty())) {
        batchDF.filter(col("Name") === "Shree").repartition(1).write.mode("append").json("/home/sathish/struct_strm/chck/shree")
      }
      if(!(batchDF.filter(! ((col("Name") === "Shree")  || (col("Name") === "Sathish"))).rdd.isEmpty())) {
        val cnt = ((batchDF.filter(!((col("Name") === "Shree") || (col("Name") === "Sathish"))).count().toDouble / 2.0).ceil).toInt
        batchDF.filter(!((col("Name") === "Shree") || (col("Name") === "Sathish"))).repartition(cnt).write.mode("append").json("/home/sathish/struct_strm/chck/others")
      }
      batchDF.unpersist()
    }

    val qry = transaction_detail_df5
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .foreachBatch(intcal _)
      .outputMode("update")
      .option("checkpointLocation", "hdfs://localhost:9000/struct_strm")
      .start()

    qry.awaitTermination()
    //spark.streams.awaitAnyTermination()

    println("Spark Structured Streaming with Kafka Demo Application Completed.")
  }
}

