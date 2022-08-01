package com.github.marija

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtil {
  def getSpark(appName:String, partitionCount:Int = 5, master: String = "local", verbose:Boolean = true): SparkSession = {
    if (verbose) println(s"$appName with Scala version: ${util.Properties.versionNumberString}")
    val sparkSession = SparkSession.builder().appName(appName).master(master).getOrCreate()
    sparkSession.conf.set("spark.sql.shuffle.partitions", partitionCount)
    if (verbose) println(s"Session started on Spark version ${sparkSession.version} with ${partitionCount} partitions")
    sparkSession
  }

  def getCSVwithView(spark: SparkSession, filepath:String,
                     header:Boolean = true,
                     source:String="csv",
                     viewName:String = "dfTable",
                     inferSchema:Boolean=true,
                     printSchema:Boolean = true): DataFrame = {
  val df = spark.read.format(source)
    .option("header", header.toString)
    .option("inferSchema", inferSchema.toString)
    .load(filepath)
    if (!viewName.isBlank) {
      df.createOrReplaceTempView(viewName)
      println(s"Created Temporary View for SQL queries called: $viewName")
    }
    if (printSchema) df.printSchema()
    df
  }
}
