package day5

import org.apache.spark.sql.SparkSession

object Day5TransformationsActions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day5TransformationsActions")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("======================================")
    println("DAY 5 - TRANSFORMATIONS AND ACTIONS")
    println("======================================")

    // 1. Create RDDs
    println("\n1. CREATE RDDs")

    val numbersRDD = sc.parallelize(List(1, 2, 3, 4, 5))
    val moreNumbersRDD = sc.parallelize(List(4, 5, 6, 7, 8))

    println(s"RDD 1: ${numbersRDD.collect().mkString(", ")}")
    println(s"RDD 2: ${moreNumbersRDD.collect().mkString(", ")}")

    // 2. map transformation
    println("\n2. MAP TRANSFORMATION")

    val squaredRDD = numbersRDD.map(number => number * number)

    println(s"Squared values: ${squaredRDD.collect().mkString(", ")}")

    // 3. filter transformation
    println("\n3. FILTER TRANSFORMATION")

    val evenRDD = numbersRDD.filter(number => number % 2 == 0)

    println(s"Even values: ${evenRDD.collect().mkString(", ")}")

    // 4. flatMap transformation
    println("\n4. FLATMAP TRANSFORMATION")

    val sentencesRDD = sc.parallelize(
      List(
        "Spark is fast",
        "Scala works with Spark"
      )
    )

    val wordsRDD = sentencesRDD.flatMap(sentence => sentence.split(" "))

    println(s"Words: ${wordsRDD.collect().mkString(", ")}")

    // 5. distinct transformation
    println("\n5. DISTINCT TRANSFORMATION")

    val distinctRDD = moreNumbersRDD.distinct()

    println(s"Distinct values: ${distinctRDD.collect().sorted.mkString(", ")}")

    // 6. union transformation
    println("\n6. UNION TRANSFORMATION")

    val unionRDD = numbersRDD.union(moreNumbersRDD)

    println(s"Union values: ${unionRDD.collect().mkString(", ")}")

    // 7. Actions
    println("\n7. ACTIONS")

    println(s"Count: ${numbersRDD.count()}")
    println(s"First value: ${numbersRDD.first()}")
    println(s"First three values: ${numbersRDD.take(3).mkString(", ")}")
    println(s"Collect: ${numbersRDD.collect().mkString(", ")}")
    println(s"Reduce sum: ${numbersRDD.reduce(_ + _)}")

    // 8. Transformations vs Actions
    println("\n8. TRANSFORMATIONS VS ACTIONS")

    println("Transformations:")
    println("map, filter, flatMap, distinct and union")

    println("\nActions:")
    println("count, collect, first, take and reduce")

    println("\nTransformations are lazy and are executed only when an action is called.")

    // 9. Log analyzer
    println("\n9. LOG ANALYZER")

    val logRDD = sc.textFile("data/day5/application.log")

    val errorLogs = logRDD.filter(line => line.contains("ERROR"))

    println(s"Total log entries: ${logRDD.count()}")
    println(s"Number of ERROR messages: ${errorLogs.count()}")

    println("\nERROR messages:")
    errorLogs.collect().foreach(println)

    // 10. Partition information
    println("\n10. PARTITION INFORMATION")

    println(s"Spark master: ${sc.master}")
    println(s"Default parallelism: ${sc.defaultParallelism}")
    println(s"Numbers RDD partitions: ${numbersRDD.getNumPartitions}")
    println(s"Log RDD partitions: ${logRDD.getNumPartitions}")

    println("\n======================================")
    println("DAY 5 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
