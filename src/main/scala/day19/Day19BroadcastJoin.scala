package day19

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast

object Day19BroadcastJoin {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day19BroadcastJoin")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("======================================")
    println("DAY 19 - BROADCAST JOIN")
    println("======================================")

    // --------------------------------------------------
    // 1. READ TRANSACTIONS AND BRANCH MASTER DATA
    // --------------------------------------------------

    println("1. READ TRANSACTIONS AND BRANCH MASTER")

    val transactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/day19/transactions.csv")

    val branches = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/day19/branches.csv")

    println("Transactions:")
    transactions.show(false)

    println("Branch Master:")
    branches.show(false)

    println("Transaction count: " + transactions.count())
    println("Branch count: " + branches.count())

    // --------------------------------------------------
    // 2. NORMAL JOIN
    // --------------------------------------------------

    println("2. NORMAL INNER JOIN")

    val normalJoin = transactions
      .join(branches, Seq("branch_id"), "inner")
      .select(
        "transaction_id",
        "branch_id",
        "customer_id",
        "amount",
        "branch_name",
        "city",
        "region"
      )

    normalJoin.show(false)

    // --------------------------------------------------
    // 3. BROADCAST JOIN
    // --------------------------------------------------

    println("3. BROADCAST JOIN")

    val broadcastJoin = transactions
      .join(
        broadcast(branches),
        Seq("branch_id"),
        "inner"
      )
      .select(
        "transaction_id",
        "branch_id",
        "customer_id",
        "amount",
        "branch_name",
        "city",
        "region"
      )

    broadcastJoin.show(false)

    // --------------------------------------------------
    // 4. REGION-WISE TRANSACTION SUMMARY
    // --------------------------------------------------

    println("4. REGION-WISE TRANSACTION SUMMARY")

    val regionSummary = broadcastJoin
      .groupBy("region")
      .agg(
        org.apache.spark.sql.functions.count("*").alias("transaction_count"),
        org.apache.spark.sql.functions.sum("amount").alias("total_amount"),
        org.apache.spark.sql.functions.avg("amount").alias("average_amount")
      )
      .orderBy("region")

    regionSummary.show(false)

    // --------------------------------------------------
    // 5. BRANCH-WISE TRANSACTION SUMMARY
    // --------------------------------------------------

    println("5. BRANCH-WISE TRANSACTION SUMMARY")

    val branchSummary = broadcastJoin
      .groupBy("branch_id", "branch_name", "city")
      .agg(
        org.apache.spark.sql.functions.count("*").alias("transaction_count"),
        org.apache.spark.sql.functions.sum("amount").alias("total_amount")
      )
      .orderBy("branch_id")

    branchSummary.show(false)

    // --------------------------------------------------
    // 6. EXPLAIN BROADCAST JOIN
    // --------------------------------------------------

    println("6. BROADCAST JOIN PHYSICAL PLAN")

    println("Physical plan for broadcast join:")

    broadcastJoin.explain(true)

    // --------------------------------------------------
    // 7. BROADCAST JOIN CONCEPT
    // --------------------------------------------------

    println("7. BROADCAST JOIN CONCEPT")

    println("Broadcast Join:")
    println("- Used when one DataFrame is small enough to fit in executor memory.")
    println("- Spark broadcasts the small DataFrame to the executors.")
    println("- The large DataFrame does not need to shuffle the small table.")
    println("- This can reduce network shuffle and improve join performance.")

    println("Why use it here?")
    println("- Transactions represent the large fact DataFrame.")
    println("- Branches represent a small reference/master DataFrame.")
    println("- The branch master has only a few rows.")
    println("- Therefore, broadcasting the branch master is appropriate.")

    // --------------------------------------------------
    // 8. BROADCAST JOIN VS SHUFFLE SORT MERGE JOIN
    // --------------------------------------------------

    println("8. BROADCAST JOIN VS SHUFFLE SORT MERGE JOIN")

    println("Broadcast Join:")
    println("- Small table is replicated to executors.")
    println("- Avoids shuffling the large DataFrame for the small side.")
    println("- Suitable for small dimension/reference tables.")

    println("Shuffle Sort Merge Join:")
    println("- Both sides may be shuffled by the join key.")
    println("- Data is sorted and matching keys are merged.")
    println("- Suitable when both DataFrames are large.")
    println("- Can involve network and disk/memory resources.")

    // --------------------------------------------------
    // 9. TRANSACTION MANAGEMENT SCENARIO
    // --------------------------------------------------

    println("9. TRANSACTION MANAGEMENT SCENARIO")

    println("- Process millions of transaction records.")
    println("- Keep branch master data as a small reference table.")
    println("- Broadcast the branch master.")
    println("- Enrich transactions with branch name, city and region.")
    println("- Generate regional and branch-level transaction reports.")

    println("======================================")
    println("DAY 19 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
