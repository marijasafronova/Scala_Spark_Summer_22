ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "Scala_Spark_Summer_22"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.3.0"



