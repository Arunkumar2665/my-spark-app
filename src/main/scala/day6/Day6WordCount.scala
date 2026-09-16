package day6

import org.apache.spark.sql.SparkSession

object Day6WordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day6WordCount")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("======================================")
    println("DAY 6 - WORD COUNT")
    println("======================================")

    // ------------------------------------------------
    // 1. BASIC WORD COUNT
    // ------------------------------------------------

    println("\n1. BASIC WORD COUNT")

    val textRDD = sc.parallelize(
      List(
        "Spark is fast",
        "Spark is powerful",
        "Scala works with Spark"
      )
    )

    val wordsRDD = textRDD
      .flatMap(line => line.split("\\s+"))

    val wordPairsRDD = wordsRDD
      .map(word => (word, 1))

    val wordCountRDD = wordPairsRDD
      .reduceByKey(_ + _)

    println("Word counts:")
    wordCountRDD
      .collect()
      .sortBy(_._1)
      .foreach {
        case (word, count) =>
          println(s"$word -> $count")
      }

    // ------------------------------------------------
    // 2. WORD COUNT PIPELINE
    // ------------------------------------------------

    println("\n2. flatMap -> map -> reduceByKey")

    println("flatMap : Split each line into individual words")
    println("map    : Convert each word into (word, 1)")
    println("reduceByKey : Add counts for the same word")

    // ------------------------------------------------
    // 3. CASE-INSENSITIVE WORD COUNT
    // ------------------------------------------------

    println("\n3. CASE-INSENSITIVE WORD COUNT")

    val caseInsensitiveCountRDD = textRDD
      .flatMap(line => line.split("\\s+"))
      .map(word => word.toLowerCase)
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    println("Case-insensitive counts:")
    caseInsensitiveCountRDD
      .collect()
      .sortBy(_._1)
      .foreach {
        case (word, count) =>
          println(s"$word -> $count")
      }

    // ------------------------------------------------
    // 4. IGNORE PUNCTUATION AND EMPTY WORDS
    // ------------------------------------------------

    println("\n4. IGNORE PUNCTUATION AND EMPTY WORDS")

    val punctuationTextRDD = sc.parallelize(
      List(
        "Spark, Spark! is fast.",
        "Scala; Spark is powerful.",
        "Spark works with Scala."
      )
    )

    val cleanedWordCountRDD = punctuationTextRDD
      .flatMap(line => line.split("\\s+"))
      .map(word => word.replaceAll("[^A-Za-z0-9]", ""))
      .map(word => word.toLowerCase)
      .filter(word => word.nonEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    println("Cleaned word counts:")
    cleanedWordCountRDD
      .collect()
      .sortBy(_._1)
      .foreach {
        case (word, count) =>
          println(s"$word -> $count")
      }

    // ------------------------------------------------
    // 5. APPLICATION LOG WORD COUNT
    // ------------------------------------------------

    println("\n5. APPLICATION LOG ANALYSIS")

    val logRDD = sc.textFile("data/day6/application.log")

    val logWordCountRDD = logRDD
      .flatMap(line => line.split("\\s+"))
      .map(word => word.replaceAll("[^A-Za-z0-9]", ""))
      .map(word => word.toLowerCase)
      .filter(word => word.nonEmpty)
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    println("\nTop 10 most frequent words in application logs:")

    logWordCountRDD
      .sortBy {
        case (_, count) => -count
      }
      .take(10)
      .foreach {
        case (word, count) =>
          println(s"$word -> $count")
      }

    // ------------------------------------------------
    // 6. PARTITION INFORMATION
    // ------------------------------------------------

    println("\n6. PARTITION INFORMATION")

    println(s"Spark master: ${sc.master}")
    println(s"Default parallelism: ${sc.defaultParallelism}")
    println(s"Log RDD partitions: ${logRDD.getNumPartitions}")
    println(s"Word count RDD partitions: ${logWordCountRDD.getNumPartitions}")

    // ------------------------------------------------
    // 7. CONCEPT SUMMARY
    // ------------------------------------------------

    println("\n7. CONCEPT SUMMARY")

    println("RDD transformations used:")
    println("flatMap, map, filter, reduceByKey, sortBy")

    println("\nKey points:")
    println("- flatMap creates individual words")
    println("- map creates key-value pairs")
    println("- reduceByKey aggregates values by key")
    println("- filter removes empty words")
    println("- reduceByKey involves a shuffle")
    println("- Word count demonstrates distributed data processing")

    println("\n======================================")
    println("DAY 6 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
