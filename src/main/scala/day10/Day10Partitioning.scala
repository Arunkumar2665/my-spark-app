package day10

import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner

object Day10Partitioning {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day10Partitioning")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("======================================")
    println("DAY 10 - PARTITIONING")
    println("======================================")

    // ------------------------------------------------
    // 1. CREATE RDD
    // ------------------------------------------------

    println("\n1. CREATE BASE RDD")

    val numbersRDD = sc.parallelize(
      1 to 20
    )

    println(s"Data: ${numbersRDD.collect().mkString(", ")}")
    println(s"Initial partitions: ${numbersRDD.getNumPartitions}")

    // ------------------------------------------------
    // 2. INSPECT PARTITIONS
    // ------------------------------------------------

    println("\n2. INSPECT PARTITIONS")

    println(s"Default parallelism: ${sc.defaultParallelism}")
    println(s"Current partition count: ${numbersRDD.getNumPartitions}")

    val partitionContents = numbersRDD.mapPartitionsWithIndex {
      case (partitionId, iterator) =>
        Iterator(
          s"Partition $partitionId -> ${iterator.mkString(", ")}"
        )
    }

    partitionContents.collect().foreach(println)

    // ------------------------------------------------
    // 3. REPARTITION
    // ------------------------------------------------

    println("\n3. REPARTITION")

    val repartitionedRDD = numbersRDD.repartition(4)

    println(s"Partitions after repartition(4): ${repartitionedRDD.getNumPartitions}")

    val repartitionContents = repartitionedRDD.mapPartitionsWithIndex {
      case (partitionId, iterator) =>
        Iterator(
          s"Partition $partitionId -> ${iterator.mkString(", ")}"
        )
    }

    repartitionContents.collect().foreach(println)

    println("repartition can increase or decrease partitions.")
    println("It performs a shuffle.")

    // ------------------------------------------------
    // 4. COALESCE
    // ------------------------------------------------

    println("\n4. COALESCE")

    val coalescedRDD = repartitionedRDD.coalesce(2)

    println(s"Partitions after coalesce(2): ${coalescedRDD.getNumPartitions}")

    val coalescedContents = coalescedRDD.mapPartitionsWithIndex {
      case (partitionId, iterator) =>
        Iterator(
          s"Partition $partitionId -> ${iterator.mkString(", ")}"
        )
    }

    coalescedContents.collect().foreach(println)

    println("coalesce is commonly used to decrease partitions.")
    println("It can avoid a full shuffle when reducing partitions.")

    // ------------------------------------------------
    // 5. PARTITION BY
    // ------------------------------------------------

    println("\n5. PARTITION BY")

    val accountRDD = sc.parallelize(
      List(
        ("ACC101", 5000),
        ("ACC102", 3000),
        ("ACC101", 2500),
        ("ACC103", 7000),
        ("ACC102", 1500),
        ("ACC101", 1000)
      )
    )

    println("Original Pair RDD:")
    accountRDD.collect().foreach(println)

    val partitionedAccountRDD = accountRDD.partitionBy(
      new HashPartitioner(3)
    )

    println(
      s"Partitions after partitionBy(HashPartitioner(3)): ${partitionedAccountRDD.getNumPartitions}"
    )

    println("Partition distribution:")

    partitionedAccountRDD
      .mapPartitionsWithIndex {
        case (partitionId, iterator) =>
          Iterator(
            s"Partition $partitionId -> ${iterator.mkString(", ")}"
          )
      }
      .collect()
      .foreach(println)

    // ------------------------------------------------
    // 6. AGGREGATION AFTER PARTITIONING
    // ------------------------------------------------

    println("\n6. AGGREGATION AFTER PARTITIONING")

    val accountTotals = partitionedAccountRDD
      .reduceByKey(_ + _)

    println("Account totals:")

    accountTotals
      .collect()
      .sortBy(_._1)
      .foreach {
        case (account, amount) =>
          println(s"$account -> $amount")
      }

    // ------------------------------------------------
    // 7. REPARTITION VS COALESCE
    // ------------------------------------------------

    println("\n7. REPARTITION VS COALESCE")

    println("repartition:")
    println("- Changes the number of partitions using a shuffle.")
    println("- Can increase or decrease partitions.")
    println("- Useful when data needs better redistribution.")

    println("\ncoalesce:")
    println("- Primarily used to decrease partitions.")
    println("- Usually avoids a full shuffle.")
    println("- Useful after filtering or reducing data volume.")

    // ------------------------------------------------
    // 8. WHEN TO INCREASE PARTITIONS
    // ------------------------------------------------

    println("\n8. WHEN TO INCREASE PARTITIONS")

    println("Increase partitions when:")
    println("- Dataset is large.")
    println("- Existing partitions are too large.")
    println("- More parallelism is needed.")
    println("- Some tasks are taking too long.")

    // ------------------------------------------------
    // 9. WHEN TO DECREASE PARTITIONS
    // ------------------------------------------------

    println("\n9. WHEN TO DECREASE PARTITIONS")

    println("Decrease partitions when:")
    println("- Dataset becomes smaller after filtering.")
    println("- Too many small partitions create overhead.")
    println("- Fewer output files are required.")

    // ------------------------------------------------
    // 10. OPTIMIZATION SCENARIO
    // ------------------------------------------------

    println("\n10. OPTIMIZATION SCENARIO")

    println("Problem:")
    println("Dataset has too few partitions.")

    println("\nSolution:")
    println("Use repartition() to increase partitions")
    println("and improve parallel processing.")

    println("\nExample:")
    println("numbersRDD.repartition(4)")

    println("\nIf the dataset becomes small after filtering:")
    println("Use coalesce() to reduce unnecessary partitions.")

    // ------------------------------------------------
    // 11. CONCEPT SUMMARY
    // ------------------------------------------------

    println("\n11. CONCEPT SUMMARY")

    println("Partition:")
    println("- A logical chunk of distributed data.")

    println("\nrepartition:")
    println("- Changes partition count using shuffle.")

    println("\ncoalesce:")
    println("- Reduces partition count with less data movement.")

    println("\npartitionBy:")
    println("- Controls partitioning of Pair RDDs using a partitioner.")

    println("\nHashPartitioner:")
    println("- Uses the key's hash value to determine the partition.")

    println("\n======================================")
    println("DAY 10 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
