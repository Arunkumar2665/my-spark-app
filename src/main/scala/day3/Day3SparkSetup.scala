package day3

import org.apache.spark.sql.SparkSession

object Day3SparkSetup {

  def main(args: Array[String]): Unit = {

    // -----------------------------------------------
    // Create SparkSession
    // -----------------------------------------------

    val cores = if (args.nonEmpty) args(0) else "2"

val spark = SparkSession.builder()
  .appName("Day3SparkSetup")
  .master(s"local[$cores]")
  .getOrCreate()

    // -----------------------------------------------
    // Get SparkContext
    // -----------------------------------------------

    val sc = spark.sparkContext

    println("======================================")
    println("DAY 3 - SPARK SETUP AND FIRST APPLICATION")
    println("======================================")

    // -----------------------------------------------
    // Spark application information
    // -----------------------------------------------

    println("\n1. SPARK APPLICATION INFORMATION")
    println(s"Requested cores : $cores")

    println(s"Application Name : ${sc.appName}")
    println(s"Master           : ${sc.master}")
    println(s"Application ID   : ${sc.applicationId}")

    // -----------------------------------------------
    // Read text file
    // -----------------------------------------------

    println("\n2. READING TEXT FILE")

    val filePath = "data/day3/sample.txt"

    val lines = sc.textFile(filePath)

    println(s"Number of lines: ${lines.count()}")

    println("\nFile contents:")

    lines.collect().foreach(println)

    // -----------------------------------------------
    // Inspect partitions
    // -----------------------------------------------

    println("\n3. PARTITION INFORMATION")
    println(s"Number of partitions: ${lines.getNumPartitions}")

    // -----------------------------------------------
    // SparkSession and SparkContext
    // -----------------------------------------------

    println("\n4. SPARK COMPONENTS")

    println("SparkSession:")
    println("Entry point for working with Spark SQL, DataFrames and Datasets.")

    println("\nSparkContext:")
    println("Main entry point for Spark core functionality and RDD operations.")

    // -----------------------------------------------
    // Driver, Executor and Cluster Manager
    // -----------------------------------------------

    println("\n5. SPARK ARCHITECTURE")

    println("Driver:")
    println("Runs the main application and coordinates Spark execution.")

    println("\nExecutor:")
    println("Runs tasks and performs computation on data partitions.")

    println("\nCluster Manager:")
    println("Allocates resources for Spark applications.")

    // -----------------------------------------------
    // Close Spark
    // -----------------------------------------------

    spark.stop()

    println("\n======================================")
    println("DAY 3 COMPLETED")
    println("======================================")
  }
}
