package day8

import org.apache.spark.sql.SparkSession

object Day8DAGExecution {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day8DAGExecution")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("======================================")
    println("DAY 8 - DAG AND SPARK EXECUTION")
    println("======================================")

    // ------------------------------------------------
    // 1. CREATE BASE RDD
    // ------------------------------------------------

    println("\n1. CREATE BASE RDD")

    val numbersRDD = sc.parallelize(
      List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    )

    println(s"Base data: ${numbersRDD.collect().mkString(", ")}")
    println(s"Partitions: ${numbersRDD.getNumPartitions}")

    // ------------------------------------------------
    // 2. NARROW TRANSFORMATIONS
    // ------------------------------------------------

    println("\n2. NARROW TRANSFORMATIONS")

    val evenRDD = numbersRDD.filter(_ % 2 == 0)

    val squaredRDD = evenRDD.map(number => number * number)

    println(s"Even values: ${evenRDD.collect().mkString(", ")}")
    println(s"Squared values: ${squaredRDD.collect().mkString(", ")}")

    println("filter and map are narrow transformations.")
    println("They do not require data to move between partitions.")

    // ------------------------------------------------
    // 3. WIDE TRANSFORMATION
    // ------------------------------------------------

    println("\n3. WIDE TRANSFORMATION")

    val pairRDD = sc.parallelize(
      List(
        ("A", 10),
        ("B", 20),
        ("A", 30),
        ("B", 40),
        ("C", 50)
      )
    )

    val reducedRDD = pairRDD.reduceByKey(_ + _)

    println("Input key-value pairs:")
    pairRDD.collect().foreach(println)

    println("\nReduced values:")
    reducedRDD.collect().sortBy(_._1).foreach(println)

    println("reduceByKey is a wide transformation because it causes a shuffle.")

    // ------------------------------------------------
    // 4. DAG
    // ------------------------------------------------

    println("\n4. DAG REPRESENTATION")

    println("Logical DAG:")
    println("Parallelize")
    println("     |")
    println("     v")
    println("  filter")
    println("     |")
    println("     v")
    println("    map")
    println("     |")
    println("     v")
    println(" reduceByKey")
    println("     |")
    println("   SHUFFLE")
    println("     |")
    println("     v")
    println("  collect")
    println("     |")
    println("     v")
    println("   Result")

    // ------------------------------------------------
    // 5. JOBS, STAGES AND TASKS
    // ------------------------------------------------

    println("\n5. JOBS, STAGES AND TASKS")

    println("Job:")
    println("- Created when an action such as collect is executed.")

    println("\nStage:")
    println("- A group of tasks separated by shuffle boundaries.")

    println("\nTask:")
    println("- A unit of work executed on one partition.")

    println("\nPartition:")
    println("- A portion of distributed data processed by one task.")

    // ------------------------------------------------
    // 6. SHUFFLE BOUNDARY
    // ------------------------------------------------

    println("\n6. SHUFFLE BOUNDARY")

    println("Before reduceByKey:")
    println("filter -> map")
    println("These are narrow transformations.")

    println("\nAt reduceByKey:")
    println("SHUFFLE BOUNDARY")

    println("\nAfter reduceByKey:")
    println("Aggregated results are sent to the final stage.")

    // ------------------------------------------------
    // 7. STAGE PREDICTION
    // ------------------------------------------------

    println("\n7. STAGE PREDICTION")

    println("Pipeline:")
    println("parallelize -> filter -> map -> reduceByKey -> collect")

    println("\nExpected execution:")
    println("Stage 0: parallelize -> filter -> map")
    println("         |")
    println("         v")
    println("      SHUFFLE")
    println("         |")
    println("         v")
    println("Stage 1: reduceByKey -> collect")

    println("\nExpected number of stages: 2")

    // ------------------------------------------------
    // 8. RDD LINEAGE
    // ------------------------------------------------

    println("\n8. RDD LINEAGE")

    println("Reduced RDD lineage:")
    println(reducedRDD.toDebugString)

    // ------------------------------------------------
    // 9. ACTION AND EXECUTION
    // ------------------------------------------------

    println("\n9. ACTION")

    val finalResult = reducedRDD
      .collect()
      .sortBy(_._1)

    println("Final result:")

    finalResult.foreach {
      case (key, value) =>
        println(s"$key -> $value")
    }

    // ------------------------------------------------
    // 10. CONCEPT SUMMARY
    // ------------------------------------------------

    println("\n10. CONCEPT SUMMARY")

    println("DAG:")
    println("- Directed Acyclic Graph representing Spark computation.")

    println("\nNarrow transformations:")
    println("- filter, map")
    println("- No shuffle between parent and child partitions.")

    println("\nWide transformations:")
    println("- reduceByKey")
    println("- Requires shuffle.")

    println("\nJob:")
    println("- Triggered by an action.")

    println("\nStage:")
    println("- Separated by shuffle boundaries.")

    println("\nTask:")
    println("- Executes work for one partition.")

    println("\nPartition:")
    println("- Basic unit of parallel processing.")

    println("\n======================================")
    println("DAY 8 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
