package com.github.marija

import org.apache.spark.sql.SparkSession

object Day17HelloSpark extends App {
  println(s"Testing Scala version: ${util.Properties.versionString}") //check Scala version like this

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate() //creating a session
  //also session is a common name for the above spark object
  println(s"Session started on Spark version ${spark.version}")

//  val myRange = spark.range(1000).toDF("number") //create a single column dataframe (table)
//  val divisibleBy5 = myRange.where("number % 5 = 0") //so similaraities with SQL and regular Scala
//  divisibleBy5.show(10) //show first 10 rows

  //TODO create range of numbers 0 to 100
  val newRange = spark.range(101).toDF("num")
  //TODO filter into numbers divisible by 10
  val divisibleBy10 = newRange.where("num % 10 = 0")
  //TODO show the results
  divisibleBy10.show()

  spark.stop() //or .close() if you want to stop the Spark engine before the program stops running
}
