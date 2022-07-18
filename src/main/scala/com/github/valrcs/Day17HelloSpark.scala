package com.github.valrcs

import org.apache.spark.sql.SparkSession

object Day17HelloSpark extends App {
  println(s"Testing Scala version: ${util.Properties.versionString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  //also session is a common name for the above spark object
  println(s"Session started on Spark version ${spark.version}")

  spark.stop() //or .close() if you want to stop the Spark engine before the program stops running
}
