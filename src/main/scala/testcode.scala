package com.datamaking.ctv

// --class com.datamaking.ctv.streaming_app_demo

import org.apache.avro.generic.GenericData
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener._
import org.apache.spark.sql.functions.from_json
import org.apache.spark.storage.StorageLevel

object testcode extends App{

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Structured Streaming with Kafka Demo")
    //.config("spark.driver.host","localhost")
    .getOrCreate()

  val df_data =Seq(
    Row("Sathish","28"),Row("Sathish","27"),Row("Sathish","26"),
    Row("Shree","28"),
    Row("Arun","24"),
    Row("Balaji","24"),Row("Vino","30"),Row("Naveen","26"),Row("Sai","8")
  )
  val df_schema = StructType(Array(StructField("Name",StringType),StructField("Age", StringType)))

  val df = spark.createDataFrame(spark.sparkContext.parallelize(df_data),df_schema)
  //val df1 = df.withColumn("Ts",lit(current_timestamp()))

  //println("part_value", df.rdd.getNumPartitions)
  //df.show()
  println(df.count())

  Thread.sleep(10000000)

}
