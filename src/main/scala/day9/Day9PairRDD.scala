package day9

import org.apache.spark.sql.SparkSession

object Day9PairRDD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day9PairRDD")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("======================================")
    println("DAY 9 - PAIR RDD")
    println("======================================")

    // ------------------------------------------------
    // 1. CREATE KEY-VALUE RDD
    // ------------------------------------------------

    println("\n1. CREATE KEY-VALUE RDD")

    val salesRDD = sc.parallelize(
      List(
        ("Laptop", 50000),
        ("Mobile", 20000),
        ("Laptop", 45000),
        ("Tablet", 30000),
        ("Mobile", 15000)
      )
    )

    println("Sales data:")
    salesRDD.collect().foreach(println)

    // ------------------------------------------------
    // 2. REDUCEBYKEY
    // ------------------------------------------------

    println("\n2. REDUCEBYKEY")

    val revenueByProduct = salesRDD
      .reduceByKey(_ + _)

    println("Revenue by product:")

    revenueByProduct
      .collect()
      .sortBy(_._1)
      .foreach {
        case (product, revenue) =>
          println(s"$product -> $revenue")
      }

    // ------------------------------------------------
    // 3. GROUPBYKEY
    // ------------------------------------------------

    println("\n3. GROUPBYKEY")

    val groupedSales = salesRDD
      .groupByKey()

    println("Sales grouped by product:")

    groupedSales
      .collect()
      .sortBy(_._1)
      .foreach {
        case (product, values) =>
          println(s"$product -> ${values.mkString(", ")}")
      }

    // ------------------------------------------------
    // 4. MAPVALUES
    // ------------------------------------------------

    println("\n4. MAPVALUES")

    val doubledRevenue = revenueByProduct
      .mapValues(revenue => revenue * 2)

    println("Doubled revenue:")

    doubledRevenue
      .collect()
      .sortBy(_._1)
      .foreach {
        case (product, revenue) =>
          println(s"$product -> $revenue")
      }

    // ------------------------------------------------
    // 5. DEPARTMENT REVENUE
    // ------------------------------------------------

    println("\n5. REVENUE BY DEPARTMENT")

    val departmentSales = sc.parallelize(
      List(
        ("Electronics", 50000),
        ("Clothing", 20000),
        ("Electronics", 30000),
        ("Groceries", 15000),
        ("Clothing", 25000),
        ("Groceries", 10000)
      )
    )

    val revenueByDepartment = departmentSales
      .reduceByKey(_ + _)

    println("Revenue by department:")

    revenueByDepartment
      .collect()
      .sortBy(_._1)
      .foreach {
        case (department, revenue) =>
          println(s"$department -> $revenue")
      }

    // ------------------------------------------------
    // 6. REDUCEBYKEY VS GROUPBYKEY
    // ------------------------------------------------

    println("\n6. REDUCEBYKEY VS GROUPBYKEY")

    println("reduceByKey:")
    println("- Performs local aggregation before shuffle.")
    println("- Transfers less data across the network.")
    println("- Usually preferred for aggregation.")

    println("\ngroupByKey:")
    println("- Groups all values for each key.")
    println("- Can transfer more data during shuffle.")
    println("- Can require more memory.")

    // ------------------------------------------------
    // 7. BANK TRANSACTION AGGREGATION
    // ------------------------------------------------

    println("\n7. BANK TRANSACTION AGGREGATION")

    val transactionsRDD = sc.parallelize(
      List(
        ("ACC101", 5000.0),
        ("ACC102", 3000.0),
        ("ACC101", 2500.0),
        ("ACC103", 7000.0),
        ("ACC102", 1500.0),
        ("ACC101", 1000.0)
      )
    )

    val totalByAccount = transactionsRDD
      .reduceByKey(_ + _)

    println("Total transactions by account:")

    totalByAccount
      .collect()
      .sortBy(_._1)
      .foreach {
        case (account, amount) =>
          println(f"$account -> $amount%.2f")
      }

    // ------------------------------------------------
    // 8. PARTITION INFORMATION
    // ------------------------------------------------

    println("\n8. PARTITION INFORMATION")

    println(s"Spark master: ${sc.master}")
    println(s"Default parallelism: ${sc.defaultParallelism}")
    println(s"Sales RDD partitions: ${salesRDD.getNumPartitions}")
    println(s"Revenue RDD partitions: ${revenueByProduct.getNumPartitions}")

    // ------------------------------------------------
    // 9. CONCEPT SUMMARY
    // ------------------------------------------------

    println("\n9. CONCEPT SUMMARY")

    println("Pair RDD:")
    println("- RDD containing key-value pairs.")

    println("\nreduceByKey:")
    println("- Aggregates values with the same key.")
    println("- Performs a shuffle.")

    println("\ngroupByKey:")
    println("- Groups all values belonging to the same key.")
    println("- Performs a shuffle.")

    println("\nmapValues:")
    println("- Modifies values while preserving keys.")

    println("\n======================================")
    println("DAY 9 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
