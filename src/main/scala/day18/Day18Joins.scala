package day18

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day18Joins {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day18Joins")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("======================================")
    println("DAY 18 - SPARK JOINS")
    println("======================================")

    // ------------------------------------------------
    // 1. READ DATA
    // ------------------------------------------------

    println("\n1. READ ORDERS, CUSTOMERS AND PAYMENTS")

    val orders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/day18/orders.csv")

    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/day18/customers.csv")

    val payments = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/day18/payments.csv")

    println("Orders:")
    orders.show()

    println("Customers:")
    customers.show()

    println("Payments:")
    payments.show()

    // ------------------------------------------------
    // 2. INNER JOIN
    // ------------------------------------------------

    println("\n2. INNER JOIN - ORDERS WITH CUSTOMERS")

    val innerJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "inner"
      )
      .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.amount")
      )

    innerJoin.show(false)

    // ------------------------------------------------
    // 3. LEFT JOIN
    // ------------------------------------------------

    println("\n3. LEFT JOIN - ALL ORDERS WITH CUSTOMER DETAILS")

    val leftJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "left"
      )
      .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.amount")
      )

    leftJoin.show(false)

    // ------------------------------------------------
    // 4. RIGHT JOIN
    // ------------------------------------------------

    println("\n4. RIGHT JOIN - ALL CUSTOMERS WITH ORDER DETAILS")

    val rightJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "right"
      )
      .select(
        col("c.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.order_id"),
        col("o.amount")
      )

    rightJoin.show(false)

    // ------------------------------------------------
    // 5. FULL OUTER JOIN
    // ------------------------------------------------

    println("\n5. FULL OUTER JOIN - ORDERS AND CUSTOMERS")

    val fullJoin = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "full"
      )
      .select(
        coalesce(col("o.customer_id"), col("c.customer_id")).alias("customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.order_id"),
        col("o.amount")
      )

    fullJoin.show(false)

    // ------------------------------------------------
    // 6. THREE-WAY JOIN
    // ------------------------------------------------

    println("\n6. THREE-WAY JOIN - ORDERS + CUSTOMERS + PAYMENTS")

    val orderCustomerPayment = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "left"
      )
      .join(
        payments.alias("p"),
        col("o.payment_id") === col("p.payment_id"),
        "left"
      )
      .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.amount"),
        col("o.payment_id"),
        col("p.payment_method"),
        col("p.payment_status")
      )

    orderCustomerPayment.show(false)

    // ------------------------------------------------
    // 7. NULL HANDLING AFTER LEFT JOIN
    // ------------------------------------------------

    println("\n7. NULL HANDLING AFTER LEFT JOIN")

    val nullHandled = orderCustomerPayment
      .withColumn(
        "payment_status",
        coalesce(col("payment_status"), lit("NO_PAYMENT_RECORD"))
      )
      .withColumn(
        "customer_name",
        coalesce(col("customer_name"), lit("UNKNOWN_CUSTOMER"))
      )

    nullHandled.show(false)

    // ------------------------------------------------
    // 8. ALIAS AND AMBIGUOUS COLUMN HANDLING
    // ------------------------------------------------

    println("\n8. ALIAS AND AMBIGUOUS COLUMN HANDLING")

    println("Table aliases 'o', 'c' and 'p' are used")
    println("to explicitly identify columns from each DataFrame.")

    val selectedColumns = orders.alias("o")
      .join(
        customers.alias("c"),
        col("o.customer_id") === col("c.customer_id"),
        "inner"
      )
      .select(
        col("o.customer_id").alias("order_customer_id"),
        col("c.customer_id").alias("master_customer_id"),
        col("o.amount"),
        col("c.customer_name")
      )

    selectedColumns.show(false)

    // ------------------------------------------------
    // 9. SHUFFLE SORT MERGE JOIN
    // ------------------------------------------------

    println("\n9. SHUFFLE SORT MERGE JOIN")

    println("For large DataFrames, Spark may use a Shuffle Sort Merge Join.")
    println("The join keys can be shuffled across partitions.")
    println("The shuffled data is sorted and matching keys are merged.")
    println("This can involve network I/O and disk/memory resources.")

    println("\nPhysical plan for the three-way join:")
    orderCustomerPayment.explain()

    // ------------------------------------------------
    // 10. JOIN CONCEPT SUMMARY
    // ------------------------------------------------

    println("\n10. JOIN CONCEPT SUMMARY")

    println("Inner Join:")
    println("- Returns rows with matching keys in both DataFrames.")

    println("\nLeft Join:")
    println("- Keeps all rows from the left DataFrame.")
    println("- Missing right-side matches become NULL.")

    println("\nRight Join:")
    println("- Keeps all rows from the right DataFrame.")
    println("- Missing left-side matches become NULL.")

    println("\nFull Outer Join:")
    println("- Keeps matching and non-matching rows from both sides.")

    println("\nAliases:")
    println("- Help avoid ambiguous column references.")

    println("\nNull Handling:")
    println("- coalesce() can replace NULL values with defaults.")

    println("\nShuffle Sort Merge Join:")
    println("- Common join strategy for larger datasets.")
    println("- Requires shuffle and sorting of join keys.")

    // ------------------------------------------------
    // 11. ORDER MANAGEMENT SCENARIO
    // ------------------------------------------------

    println("\n11. ORDER MANAGEMENT SCENARIO")

    println("- Join orders with customer master data.")
    println("- Join orders with payment information.")
    println("- Use aliases to avoid ambiguous columns.")
    println("- Handle missing customer/payment records.")
    println("- Understand shuffle-based join execution.")

    println("\n======================================")
    println("DAY 18 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
