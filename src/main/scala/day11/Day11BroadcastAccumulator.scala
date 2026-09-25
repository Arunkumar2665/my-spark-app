package day11

import org.apache.spark.sql.SparkSession

object Day11BroadcastAccumulator {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day11BroadcastAccumulator")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("======================================")
    println("DAY 11 - BROADCAST AND ACCUMULATORS")
    println("======================================")

    // ------------------------------------------------
    // 1. SMALL PRODUCT REFERENCE DATA
    // ------------------------------------------------

    println("\n1. PRODUCT REFERENCE DATA")

    val productReference = Map(
      "P101" -> ("Laptop", 50000.0),
      "P102" -> ("Mobile", 20000.0),
      "P103" -> ("Tablet", 30000.0),
      "P104" -> ("Monitor", 15000.0)
    )

    productReference.foreach {
      case (productId, (productName, price)) =>
        println(s"$productId -> $productName -> $price")
    }

    // ------------------------------------------------
    // 2. BROADCAST THE REFERENCE MAP
    // ------------------------------------------------

    println("\n2. BROADCAST VARIABLE")

    val broadcastProducts = sc.broadcast(productReference)

    println("Product reference map has been broadcast.")
    println("Executors can read the broadcast data locally.")

    // ------------------------------------------------
    // 3. READ TRANSACTIONS
    // ------------------------------------------------

    println("\n3. READ TRANSACTIONS")

    val transactionsRDD = sc.textFile("data/day11/transactions.csv")

    val transactionDataRDD = transactionsRDD
      .filter(line => !line.startsWith("transaction_id"))
      .map { line =>
        val fields = line.split(",")

        (
          fields(0),
          fields(1),
          fields(2).toInt
        )
      }

    transactionDataRDD.collect().foreach(println)

    // ------------------------------------------------
    // 4. ACCUMULATOR FOR BAD RECORDS
    // ------------------------------------------------

    println("\n4. BAD RECORD ACCUMULATOR")

    val badRecordAccumulator = sc.longAccumulator("Bad Records")

    // ------------------------------------------------
    // 5. VALIDATE TRANSACTIONS USING BROADCAST DATA
    // ------------------------------------------------

    val validTransactionsRDD = transactionDataRDD.flatMap {
      case (transactionId, productId, quantity) =>

        val products = broadcastProducts.value

        products.get(productId) match {

          case Some((productName, price)) =>

            val totalAmount = quantity * price

            Some(
              (
                transactionId,
                productId,
                productName,
                quantity,
                totalAmount
              )
            )

          case None =>

            badRecordAccumulator.add(1)

            println(
              s"Invalid product: transaction=$transactionId product=$productId"
            )

            None
        }
    }

    // ------------------------------------------------
    // 6. TRIGGER EXECUTION
    // ------------------------------------------------

    val validTransactions = validTransactionsRDD.collect()

    println("\n5. VALID TRANSACTIONS")

    validTransactions.foreach {
      case (transactionId, productId, productName, quantity, totalAmount) =>
        println(
          f"$transactionId -> $productId -> $productName -> quantity=$quantity -> amount=$totalAmount%.2f"
        )
    }

    // ------------------------------------------------
    // 7. ACCUMULATOR RESULT
    // ------------------------------------------------

    println("\n6. ACCUMULATOR RESULT")

    println(
      s"Number of bad records: ${badRecordAccumulator.value}"
    )

    // ------------------------------------------------
    // 8. TOTAL VALID TRANSACTION VALUE
    // ------------------------------------------------

    val totalRevenue = validTransactionsRDD
      .map {
        case (_, _, _, _, totalAmount) =>
          totalAmount
      }
      .sum()

    println("\n7. VALID TRANSACTION REVENUE")

    println(f"Total valid transaction revenue: $totalRevenue%.2f")

    // ------------------------------------------------
    // 9. WHY NOT USE A NORMAL DRIVER VARIABLE?
    // ------------------------------------------------

    println("\n8. NORMAL VARIABLE VS ACCUMULATOR")

    println("A normal driver variable should not be used")
    println("to collect updates from distributed executor tasks.")

    println("An accumulator is designed for supported distributed")
    println("counter/metric updates that are aggregated back to the driver.")

    // ------------------------------------------------
    // 10. BROADCAST BENEFIT
    // ------------------------------------------------

    println("\n9. BROADCAST BENEFIT")

    println("Broadcast variables are useful when:")
    println("- Reference data is small.")
    println("- Many tasks need the same read-only data.")
    println("- We want to avoid repeatedly sending the same data")
    println("  with every task.")

    // ------------------------------------------------
    // 11. CONCEPT SUMMARY
    // ------------------------------------------------

    println("\n10. CONCEPT SUMMARY")

    println("Broadcast:")
    println("- Sends a read-only variable efficiently to executors.")

    println("\nAccumulator:")
    println("- Supports distributed updates to counters/metrics.")

    println("\nApplication scenario:")
    println("- Broadcast product master data.")
    println("- Validate transactions against the master data.")
    println("- Count invalid transactions using an accumulator.")

    println("\n======================================")
    println("DAY 11 COMPLETED")
    println("======================================")

    broadcastProducts.destroy()

    spark.stop()
  }
}
