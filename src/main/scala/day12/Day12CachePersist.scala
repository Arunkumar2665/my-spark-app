package day12

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Day12CachePersist {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day12CachePersist")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("======================================")
    println("DAY 12 - CACHE AND PERSIST")
    println("======================================")

    // ------------------------------------------------
    // 1. READ TRANSACTION DATA
    // ------------------------------------------------

    println("\n1. READ TRANSACTION DATA")

    val transactionsRDD = sc.textFile("data/day12/transactions.csv")

    val cleanedTransactionsRDD = transactionsRDD
      .filter(line => !line.startsWith("transaction_id"))
      .map { line =>
        val fields = line.split(",")

        (
          fields(0),
          fields(1),
          fields(2).toDouble,
          fields(3)
        )
      }
      .filter {
        case (_, _, _, status) =>
          status == "VALID"
      }

    println("Input transaction records: " +
      transactionsRDD.filter(!_.startsWith("transaction_id")).count())

    // ------------------------------------------------
    // 2. CACHE THE CLEANED RDD
    // ------------------------------------------------

    println("\n2. CACHE THE CLEANED TRANSACTION RDD")

    cleanedTransactionsRDD.cache()

    println("RDD cached using cache().")
    println(
      s"Storage level after cache: ${cleanedTransactionsRDD.getStorageLevel}"
    )

    // ------------------------------------------------
    // 3. FIRST ACTION - COUNT
    // ------------------------------------------------

    println("\n3. FIRST ACTION - VALID TRANSACTION COUNT")

    val validCount = cleanedTransactionsRDD.count()

    println(s"Valid transaction count: $validCount")

    // ------------------------------------------------
    // 4. SECOND ACTION - TOTAL REVENUE
    // ------------------------------------------------

    println("\n4. SECOND ACTION - TOTAL REVENUE")

    val totalRevenue = cleanedTransactionsRDD
      .map {
        case (_, _, amount, _) =>
          amount
      }
      .sum()

    println(f"Total valid transaction revenue: $totalRevenue%.2f")

    // ------------------------------------------------
    // 5. THIRD ACTION - AVERAGE TRANSACTION VALUE
    // ------------------------------------------------

    println("\n5. THIRD ACTION - AVERAGE TRANSACTION VALUE")

    val averageRevenue =
      if (validCount > 0)
        totalRevenue / validCount
      else
        0.0

    println(f"Average valid transaction value: $averageRevenue%.2f")

    // ------------------------------------------------
    // 6. CACHE BENEFIT
    // ------------------------------------------------

    println("\n6. CACHE BENEFIT")

    println("The cleaned transaction RDD is reused by multiple actions.")

    println("- Action 1: Count valid transactions")
    println("- Action 2: Calculate total revenue")
    println("- Action 3: Calculate average transaction value")

    println("Caching avoids recomputing the cleaned RDD for each action.")

    // ------------------------------------------------
    // 7. PERSIST WITH MEMORY_ONLY
    // ------------------------------------------------

    println("\n7. PERSIST WITH MEMORY_ONLY")

    val memoryOnlyRDD = cleanedTransactionsRDD
      .map {
        case (transactionId, customerId, amount, status) =>
          (transactionId, customerId, amount, status)
      }
      .persist(StorageLevel.MEMORY_ONLY)

    println(
      s"Storage level: ${memoryOnlyRDD.getStorageLevel}"
    )

    val memoryOnlyCount = memoryOnlyRDD.count()

    println(s"MEMORY_ONLY record count: $memoryOnlyCount")

    // ------------------------------------------------
    // 8. PERSIST WITH MEMORY_AND_DISK
    // ------------------------------------------------

    println("\n8. PERSIST WITH MEMORY_AND_DISK")

    val memoryAndDiskRDD = cleanedTransactionsRDD
      .map {
        case (transactionId, customerId, amount, status) =>
          (transactionId, customerId, amount, status)
      }
      .persist(StorageLevel.MEMORY_AND_DISK)

    println(
      s"Storage level: ${memoryAndDiskRDD.getStorageLevel}"
    )

    val memoryAndDiskCount = memoryAndDiskRDD.count()

    println(s"MEMORY_AND_DISK record count: $memoryAndDiskCount")

    // ------------------------------------------------
    // 9. CACHE VS PERSIST
    // ------------------------------------------------

    println("\n9. CACHE VS PERSIST")

    println("cache(): Uses the default storage level MEMORY_ONLY.")
    println("persist(): Allows choosing a storage level.")

    println("Examples:")
    println("- MEMORY_ONLY")
    println("- MEMORY_AND_DISK")
    println("- DISK_ONLY")

    // ------------------------------------------------
    // 10. WHEN CACHING CAN HURT
    // ------------------------------------------------

    println("\n10. WHEN CACHING CAN HURT PERFORMANCE")

    println("- Dataset is used only once.")
    println("- Dataset is very small and recomputation is cheaper.")
    println("- Dataset is too large for available memory.")
    println("- Excessive caching causes memory pressure and eviction.")
    println("- Many unnecessary cached datasets consume cluster resources.")

    // ------------------------------------------------
    // 11. CLEANUP
    // ------------------------------------------------

    println("\n11. UNPERSIST")

    memoryOnlyRDD.unpersist()
    memoryAndDiskRDD.unpersist()
    cleanedTransactionsRDD.unpersist()

    println("Cached and persisted RDDs have been released.")

    // ------------------------------------------------
    // 12. SCENARIO SUMMARY
    // ------------------------------------------------

    println("\n12. SCENARIO SUMMARY")

    println("Scenario: Reuse a cleaned transaction dataset in three reports.")

    println("- Clean invalid transaction records.")
    println("- Cache the cleaned dataset.")
    println("- Generate count, revenue and average-value reports.")
    println("- Demonstrate MEMORY_ONLY and MEMORY_AND_DISK persistence.")
    println("- Release cached data using unpersist().")

    println("\n======================================")
    println("DAY 12 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
