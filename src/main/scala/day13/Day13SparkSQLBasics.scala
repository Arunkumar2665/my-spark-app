package day13

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day13SparkSQLBasics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day13SparkSQLBasics")
      .master("local[2]")
      .getOrCreate()

    println("======================================")
    println("DAY 13 - SPARK SQL BASICS")
    println("======================================")

    // ------------------------------------------------
    // 1. CREATE DATAFRAME FROM CSV
    // ------------------------------------------------

    println("\n1. CREATE DATAFRAME FROM CSV")

    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/day13/customers.csv")

    println("Customer DataFrame created successfully.")

    customersDF.show(false)

    // ------------------------------------------------
    // 2. INSPECT SCHEMA
    // ------------------------------------------------

    println("\n2. DATAFRAME SCHEMA")

    customersDF.printSchema()

    // ------------------------------------------------
    // 3. SELECT COLUMNS
    // ------------------------------------------------

    println("\n3. SELECT CUSTOMER COLUMNS")

    customersDF
      .select("customer_id", "name", "city", "total_spend")
      .show(false)

    // ------------------------------------------------
    // 4. FILTER CUSTOMERS
    // ------------------------------------------------

    println("\n4. FILTER HIGH VALUE CUSTOMERS")

    val highValueCustomers = customersDF
      .filter(col("total_spend") >= 80000)

    highValueCustomers
      .select("customer_id", "name", "city", "total_spend")
      .show(false)

    println(
      s"High value customer count: ${highValueCustomers.count()}"
    )

    // ------------------------------------------------
    // 5. WITHCOLUMN - CUSTOMER CATEGORY
    // ------------------------------------------------

    println("\n5. WITHCOLUMN - CUSTOMER SPEND CATEGORY")

    val customerAnalyticsDF = customersDF
      .withColumn(
        "spend_category",
        when(col("total_spend") >= 100000, "PREMIUM")
          .when(col("total_spend") >= 50000, "STANDARD")
          .otherwise("BASIC")
      )

    customerAnalyticsDF
      .select(
        "customer_id",
        "name",
        "city",
        "total_spend",
        "spend_category"
      )
      .show(false)

    // ------------------------------------------------
    // 6. REGISTER TEMPORARY VIEW
    // ------------------------------------------------

    println("\n6. CREATE TEMPORARY VIEW")

    customerAnalyticsDF.createOrReplaceTempView("customers")

    println("Temporary view 'customers' created successfully.")

    // ------------------------------------------------
    // 7. SPARK SQL QUERY
    // ------------------------------------------------

    println("\n7. SPARK SQL - HIGH VALUE CUSTOMERS")

    val highValueSQL = spark.sql(
      """
        SELECT customer_id, name, city, total_spend, spend_category
        FROM customers
        WHERE total_spend >= 80000
        ORDER BY total_spend DESC
      """
    )

    highValueSQL.show(false)

    // ------------------------------------------------
    // 8. CITY-WISE CUSTOMER ANALYTICS
    // ------------------------------------------------

    println("\n8. SPARK SQL - CITY-WISE CUSTOMER ANALYTICS")

    val cityAnalyticsSQL = spark.sql(
      """
        SELECT
          city,
          COUNT(*) AS customer_count,
          ROUND(AVG(total_spend), 2) AS average_spend,
          ROUND(SUM(total_spend), 2) AS total_spend
        FROM customers
        GROUP BY city
        ORDER BY total_spend DESC
      """
    )

    cityAnalyticsSQL.show(false)

    // ------------------------------------------------
    // 9. PREMIUM CUSTOMER REPORT
    // ------------------------------------------------

    println("\n9. SPARK SQL - PREMIUM CUSTOMER REPORT")

    val premiumCustomersSQL = spark.sql(
      """
        SELECT
          customer_id,
          name,
          city,
          total_spend
        FROM customers
        WHERE spend_category = 'PREMIUM'
        ORDER BY total_spend DESC
      """
    )

    premiumCustomersSQL.show(false)

    // ------------------------------------------------
    // 10. DATAFRAME VS SQL SUMMARY
    // ------------------------------------------------

    println("\n10. SPARK SQL CONCEPT SUMMARY")

    println("- DataFrame provides a structured distributed dataset.")
    println("- printSchema() displays column names and data types.")
    println("- select() chooses required columns.")
    println("- filter() selects records matching a condition.")
    println("- withColumn() creates or transforms columns.")
    println("- Temporary views allow SQL queries on DataFrames.")
    println("- Spark SQL supports analytical queries using SQL syntax.")

    // ------------------------------------------------
    // 11. CUSTOMER ANALYTICS SCENARIO
    // ------------------------------------------------

    println("\n11. CUSTOMER ANALYTICS SCENARIO")

    println("- Load customer data from CSV.")
    println("- Inspect the customer schema.")
    println("- Filter high-value customers.")
    println("- Create customer spend categories.")
    println("- Register a temporary SQL view.")
    println("- Generate city-wise customer analytics.")
    println("- Generate a premium customer report.")

    println("\n======================================")
    println("DAY 13 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
