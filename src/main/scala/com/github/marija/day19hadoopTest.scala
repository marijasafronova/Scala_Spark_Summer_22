package com.github.marija

import org.apache.spark.sql.SparkSession

object day19hadoopTest extends App {
  println(s"Hadoop test with Scala version: ${util.Properties.versionNumberString}")
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

}
