package com.github.marija
import org.apache.spark.sql.functions.{col, udf}
object day27 extends App {

  val spark = SparkUtil.getSpark("Sparky")

  val df = spark.range(10).toDF("num")
  df.printSchema()
  df.show()

  def power3 (n: Double):Double = n*n*n
  println(power3(10))

  def power3int(n: Long):Long = n*n*n


  val power3udf = udf(power3(_:Double):Double) //the type is user defined function
  val power3intudf = udf(power3int(_:Long):Long) //the type is user defined function

  df.withColumn("numCubed", power3udf(col("num")))
    .withColumn("numCubedInteger", power3intudf(col("num")))
    .show()


  spark.udf.register("power3", power3(_:Double):Double)

  df.selectExpr("power3(num)").show(5)

  spark.udf.register("power3int", power3int(_:Long):Long)

  df.createOrReplaceTempView("dfTable")

  spark.sql(
    """
      |SELECT *,
      |power3(num),
      |power3int(num)
      |FROM dfTable
      |""".stripMargin)
    .show()
}
