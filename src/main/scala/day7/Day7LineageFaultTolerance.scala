package day7

import org.apache.spark.sql.SparkSession

object Day7LineageFaultTolerance {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day7LineageFaultTolerance")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("======================================")
    println("DAY 7 - IMMUTABILITY, LINEAGE AND FAULT TOLERANCE")
    println("======================================")

    // ------------------------------------------------
    // 1. CREATE BASE RDD
    // ------------------------------------------------

    println("\n1. CREATE BASE RDD")

    val numbersRDD = sc.parallelize(
      List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    )

    println(s"Base RDD: ${numbersRDD.collect().mkString(", ")}")
    println(s"Partitions: ${numbersRDD.getNumPartitions}")

    // ------------------------------------------------
    // 2. TRANSFORMATION CHAIN
    // ------------------------------------------------

    println("\n2. MULTI-STEP TRANSFORMATION CHAIN")

    val evenRDD = numbersRDD.filter(_ % 2 == 0)

    val squaredRDD = evenRDD.map(number => number * number)

    val greaterThanTenRDD = squaredRDD.filter(_ > 10)

    println(s"Even values: ${evenRDD.collect().mkString(", ")}")
    println(s"Squared values: ${squaredRDD.collect().mkString(", ")}")
    println(s"Values greater than 10: ${greaterThanTenRDD.collect().mkString(", ")}")

    // ------------------------------------------------
    // 3. RDD IMMUTABILITY
    // ------------------------------------------------

    println("\n3. RDD IMMUTABILITY")

    println("Original RDD remains unchanged:")
    println(s"numbersRDD: ${numbersRDD.collect().mkString(", ")}")

    println("Each transformation creates a new RDD.")
    println("numbersRDD -> evenRDD -> squaredRDD -> greaterThanTenRDD")

    // ------------------------------------------------
    // 4. LINEAGE
    // ------------------------------------------------

    println("\n4. RDD LINEAGE")

    println("Lineage chain:")
    println("Parallelize")
    println("    |")
    println("    v")
    println("numbersRDD")
    println("    |")
    println("  filter")
    println("    |")
    println("    v")
    println("evenRDD")
    println("    |")
    println("   map")
    println("    |")
    println("    v")
    println("squaredRDD")
    println("    |")
    println("  filter")
    println("    |")
    println("    v")
    println("greaterThanTenRDD")

    println("\nRDD Debug String:")
    println(greaterThanTenRDD.toDebugString)

    // ------------------------------------------------
    // 5. ACTION
    // ------------------------------------------------

    println("\n5. ACTION")

    val result = greaterThanTenRDD.collect()

    println(s"Final result: ${result.mkString(", ")}")

    // ------------------------------------------------
    // 6. FAULT TOLERANCE
    // ------------------------------------------------

    println("\n6. FAULT TOLERANCE")

    println("RDDs are fault tolerant because Spark stores lineage information.")
    println("If a partition is lost, Spark can recompute that partition")
    println("by replaying the required transformations from the lineage.")

    println("\nConceptual recovery:")
    println("Lost partition")
    println("     |")
    println("     v")
    println("Use lineage information")
    println("     |")
    println("     v")
    println("Recompute required transformations")
    println("     |")
    println("     v")
    println("Recover partition")

    // ------------------------------------------------
    // 7. PERSISTENCE
    // ------------------------------------------------

    println("\n7. REUSING AN RDD")

    val cachedRDD = numbersRDD
      .map(number => number * 10)
      .filter(_ > 30)

    println(s"Reusable RDD: ${cachedRDD.collect().mkString(", ")}")
    println("Spark can recompute transformations when an RDD is not persisted.")

    // ------------------------------------------------
    // 8. CONCEPT SUMMARY
    // ------------------------------------------------

    println("\n8. CONCEPT SUMMARY")

    println("RDD:")
    println("- Resilient Distributed Dataset")

    println("\nImmutability:")
    println("- Existing RDDs cannot be changed.")
    println("- Transformations create new RDDs.")

    println("\nLineage:")
    println("- Records the sequence of transformations used to create an RDD.")

    println("\nFault Tolerance:")
    println("- Lost partitions can be recomputed using lineage.")

    println("\nLazy Evaluation:")
    println("- Transformations are not executed immediately.")
    println("- An action triggers execution.")

    println("\n======================================")
    println("DAY 7 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
