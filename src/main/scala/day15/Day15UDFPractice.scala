package day15

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day15UDFPractice {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day15UDFPractice")
      .master("local[2]")
      .getOrCreate()

    println("======================================")
    println("DAY 15 - UDF PRACTICE")
    println("======================================")

    // ------------------------------------------------
    // 1. READ CUSTOMER TRANSACTIONS
    // ------------------------------------------------

    println("\n1. READ CUSTOMER TRANSACTIONS")

    val customersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/day15/customers.csv")

    customersDF.show(false)

    // ------------------------------------------------
    // 2. DEFINE SCALA UDF
    // ------------------------------------------------

    println("\n2. DEFINE SCALA UDF")

    def classifyRisk(amount: Double): String = {
      if (amount >= 100000)
        "HIGH_RISK"
      else if (amount >= 50000)
        "MEDIUM_RISK"
      else
        "LOW_RISK"
    }

    val riskUDF = udf(classifyRisk _)

    println("Scala UDF 'classifyRisk' created successfully.")

    // ------------------------------------------------
    // 3. APPLY UDF WITH WITHCOLUMN
    // ------------------------------------------------

    println("\n3. APPLY UDF USING WITHCOLUMN")

    val customerRiskDF = customersDF
      .withColumn(
        "risk_category",
        riskUDF(col("transaction_amount").cast("double"))
      )

    customerRiskDF.show(false)

    // ------------------------------------------------
    // 4. DISPLAY RISK SUMMARY
    // ------------------------------------------------

    println("\n4. RISK CATEGORY SUMMARY")

    customerRiskDF
      .groupBy("risk_category")
      .count()
      .orderBy("risk_category")
      .show(false)

    // ------------------------------------------------
    // 5. BUILT-IN FUNCTION COMPARISON
    // ------------------------------------------------

    println("\n5. UDF VS BUILT-IN SPARK FUNCTION")

    val builtInClassificationDF = customersDF
      .withColumn(
        "built_in_category",
        when(col("transaction_amount") >= 100000, "HIGH_RISK")
          .when(col("transaction_amount") >= 50000, "MEDIUM_RISK")
          .otherwise("LOW_RISK")
      )

    builtInClassificationDF.show(false)

    println("Built-in Spark functions can often replace simple UDF logic.")
    println("Built-in expressions are generally preferred because Spark")
    println("can optimize them more effectively.")

    // ------------------------------------------------
    // 6. REGISTER UDF WITH SPARK CATALOG
    // ------------------------------------------------

    println("\n6. REGISTER UDF WITH SPARK CATALOG")

    spark.udf.register(
      "classifyRisk",
      (amount: Double) => classifyRisk(amount)
    )

    println("UDF 'classifyRisk' registered successfully.")

    // ------------------------------------------------
    // 7. CREATE TEMPORARY VIEW
    // ------------------------------------------------

    println("\n7. CREATE CUSTOMER VIEW")

    customersDF.createOrReplaceTempView("customer_transactions")

    println("Temporary view 'customer_transactions' created.")

    // ------------------------------------------------
    // 8. USE REGISTERED UDF IN SQL
    // ------------------------------------------------

    println("\n8. USE REGISTERED UDF IN SPARK SQL")

    val sqlRiskDF = spark.sql(
      """
        SELECT
          customer_id,
          name,
          transaction_amount,
          classifyRisk(CAST(transaction_amount AS DOUBLE)) AS risk_category
        FROM customer_transactions
        ORDER BY transaction_amount DESC
      """
    )

    sqlRiskDF.show(false)

    // ------------------------------------------------
    // 9. HIGH RISK CUSTOMERS
    // ------------------------------------------------

    println("\n9. HIGH RISK CUSTOMER REPORT")

    sqlRiskDF
      .filter(col("risk_category") === "HIGH_RISK")
      .show(false)

    // ------------------------------------------------
    // 10. UDF CONCEPT SUMMARY
    // ------------------------------------------------

    println("\n10. UDF CONCEPT SUMMARY")

    println("- A UDF allows custom Scala logic to be used with Spark columns.")
    println("- withColumn() can add a calculated column using a UDF.")
    println("- Spark UDFs can also be registered for SQL queries.")
    println("- Built-in Spark functions are generally preferred when possible.")
    println("- Built-in functions allow Spark to optimize expressions more effectively.")

    // ------------------------------------------------
    // 11. CUSTOMER RISK SCENARIO
    // ------------------------------------------------

    println("\n11. CUSTOMER RISK SCENARIO")

    println("- Read customer transaction values.")
    println("- Classify customers using a Scala UDF.")
    println("- Add risk_category using withColumn().")
    println("- Compare UDF logic with built-in Spark expressions.")
    println("- Register the UDF with the Spark catalog.")
    println("- Use the registered UDF in Spark SQL.")
    println("- Generate a high-risk customer report.")

    println("\n======================================")
    println("DAY 15 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
