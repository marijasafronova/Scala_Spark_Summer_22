package com.github.marija

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col

import scala.util.Random

object day22exercise extends App {

  //TODO open up 2014-summary.json file
  val spark = SparkUtil.getSpark("Exercise")

  val flightPath = "src/resources/flight-data/json/2014-summary.json"

  val df = spark.read.format("json")
    .load(flightPath)

  //TODO Task 1 - Filter only flights FROM US thathappened more than 10 times

 // val USdf = df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME", "count")
  val USdf = df.where(col("DEST_COUNTRY_NAME") =!= "United States")
    .where(col("count")>10)
    .distinct()
  USdf.show()

  //TODO Task 2 - I want a random sample from all 2014 of roughly 30 percent, you can use a fixed seed

  val seed = Random.nextInt()
  val withReplacement = false
  val fraction = 0.3

  val sample = df.sample(withReplacement, fraction, seed)
  sample.show()
  println(s"We got ${sample.count()} samples")
  println()
  //subtask I want to see the actual row count

  //if we want to show all the rows using fixed seed
//why can not "show overloaded" be shown??
//  val seed2 = 5
//  val withReplacement2 = false
//  val fraction2 = 0.3
//
//  val sample2 = df.sample(withReplacement, fraction, seed)
//  val sampleCount = sample2.count()
//  sample2.show(sampleCount, sampleCount, false)


  //TODO Task 3 - I want a split of full 2014 dataframe into 3 Dataframes with the following proportions 2,9, 5
  val dataFrames = df.randomSplit(Array(2,9,5), seed)

  def getDataFrameStats(dFrames:Array[Dataset[Row]], df:DataFrame): Array[Long] = {
    dFrames.map(d => d.count() * 100 / df.count())
  }
  getDataFrameStats(dataFrames, df).foreach(println)
  //subtask I want to see the row count for these dataframes and percentages

  for ((dFrame, i) <- dataFrames.zipWithIndex) {
    (println(s"$i dataframe consists of ${dFrame.count} rows"))
  }

}
