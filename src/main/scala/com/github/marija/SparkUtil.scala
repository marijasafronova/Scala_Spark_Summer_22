package com.github.marija

import org.apache.spark.sql.SparkSession

object SparkUtil {
  def getSpark(appName:String, partitionCount:Int = 5, master: String = "local", verbose:Boolean = true): SparkSession = {
    if (verbose) println(s"$appName with Scala version: ${util.Properties.versionNumberString}")
    val sparkSession = SparkSession.builder().appName(appName).master(master).getOrCreate()
    sparkSession.conf.set("spark.sql.shuffle.partitions", partitionCount)
    if (verbose) println(s"Session started on Spark version ${sparkSession.version} with ${partitionCount} partitions")
    sparkSession
  }

}
