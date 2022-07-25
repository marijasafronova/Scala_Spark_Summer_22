package com.github.marija

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format}

object day19machineLearning extends App {
  println(s"Machine Learning with Scala version: ${util.Properties.versionNumberString}")
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

  val staticDataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true") //we are letting Spark figure out the type of data in our CSVs
    .csv("src/resources/retail-data/by-day/*.csv")//we are loading anything, does not work
  //.csv("src/resources/retail-data/by-day/2010-12-01.csv")
  staticDataFrame.createOrReplaceTempView("retail_data")
  val staticSchema = staticDataFrame.schema
  println(staticSchema.toArray.mkString("\n"))
  println(s"We got ${staticDataFrame.count()} rows of data!")
// in machine learning a ot of time is spent creating new datasets
  val preppedDataFrame = staticDataFrame
    .na.fill(0) // we are filling 0s where no value exist because ML hates empty data
    .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))
    .coalesce(5)


  val trainDataFrame = preppedDataFrame
    .where("InvoiceDate < '2011-07-01'")
    //.where("InvoiceNo < '536572'") //quick and dirty split on string column
  val testDataFrame = preppedDataFrame
    .where("InvoiceDate >= '2010-12-24'")
  // .where("InvoiceNo >= '536572'")

  //splits are usually 80% training and 20% testing

  println(s"Training set is ${trainDataFrame.count()} rows")
  println(s"Test set is ${testDataFrame.count()} rows")

  val indexer = new StringIndexer()
    .setInputCol("day_of_week")
    .setOutputCol("day_of_week_index")

  val encoder = new OneHotEncoder()
    .setInputCol("day_of_week_index")
    .setOutputCol("day_of_week_encoded")

  val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
    .setOutputCol("features")

  val transformationPipeline = new Pipeline()
    .setStages(Array(indexer, encoder, vectorAssembler))

  trainDataFrame.show(5)

  // in Scala
  val fittedPipeline = transformationPipeline.fit(trainDataFrame)

  val transformedTraining = fittedPipeline.transform(trainDataFrame)

  transformedTraining.show(10)

  transformedTraining.cache()

  val kmeans = new KMeans()
    .setK(20) //so we are giving configuration for this kmeans model to create 20 clusters
    //the above number is so called hyperparameter and you would want to explore different values
    //when you do not know how many clusters there could be
    //theoretically you could have up to n clusters when you have n rows of data/features, but that would be useless
    .setSeed(1L)
  val kmModel = kmeans.fit(transformedTraining) //modelling work happens here
//  kmModel.computeCost(transformedTraining)


  //here we apply our trained model we just did
  //to the our test data set which we set aside for well testing purposes
  val transformedTest = fittedPipeline.transform(testDataFrame)

  println("Our kmeans test results")
  transformedTest.show()

  transformedTest.coalesce(1)
    //.select("Quantity", "day_of_week", "day_of_week_index", "InvoiceNo", "features")
    .select("Quantity", "day_of_week", "day_of_week_index", "InvoiceNo")
    .write
    .option("header", "true")
    .option("sep", ",")
    .mode("overwrite")
    .csv("src/resources/csv/kmeans-results.csv")
}
