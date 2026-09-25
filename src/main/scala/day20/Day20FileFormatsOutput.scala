package day20

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day20FileFormatsOutput {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day20FileFormatsOutput")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("======================================")
    println("DAY 20 - FILE FORMATS AND OUTPUT")
    println("======================================")

    // --------------------------------------------------
    // 1. READ CSV
    // --------------------------------------------------

    println("1. READ SALES DATA FROM CSV")

    val sales = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/day20/sales.csv")

    sales.show(false)

    println("Sales record count: " + sales.count())

    println("Schema:")
    sales.printSchema()

    // --------------------------------------------------
    // 2. ADD DATE COMPONENTS
    // --------------------------------------------------

    println("2. CREATE YEAR, MONTH AND DAY COLUMNS")

    val salesWithDateParts = sales
      .withColumn("sale_date", to_date(col("sale_date")))
      .withColumn("year", year(col("sale_date")))
      .withColumn("month", month(col("sale_date")))
      .withColumn("day", dayofmonth(col("sale_date")))

    salesWithDateParts.show(false)

    // --------------------------------------------------
    // 3. WRITE CSV
    // --------------------------------------------------

    println("3. WRITE DATA AS CSV")

    salesWithDateParts.write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/day20/output/csv")

    println("CSV output written to: data/day20/output/csv")

    // --------------------------------------------------
    // 4. WRITE JSON
    // --------------------------------------------------

    println("4. WRITE DATA AS JSON")

    salesWithDateParts.write
      .mode("overwrite")
      .json("data/day20/output/json")

    println("JSON output written to: data/day20/output/json")

    // --------------------------------------------------
    // 5. WRITE PARQUET
    // --------------------------------------------------

    println("5. WRITE DATA AS PARQUET")

    salesWithDateParts.write
      .mode("overwrite")
      .parquet("data/day20/output/parquet")

    println("Parquet output written to: data/day20/output/parquet")

    // --------------------------------------------------
    // 6. READ PARQUET BACK
    // --------------------------------------------------

    println("6. READ PARQUET DATA BACK")

    val parquetSales = spark.read
      .parquet("data/day20/output/parquet")

    parquetSales.show(false)

    println("Parquet record count: " + parquetSales.count())

    // --------------------------------------------------
    // 7. REPARTITION BEFORE WRITING
    // --------------------------------------------------

    println("7. REPARTITION BEFORE WRITING")

    val repartitionedSales = salesWithDateParts
      .repartition(2)

    println(
      "Partitions before repartition: " +
        salesWithDateParts.rdd.getNumPartitions
    )

    println(
      "Partitions after repartition: " +
        repartitionedSales.rdd.getNumPartitions
    )

    // --------------------------------------------------
    // 8. PARTITIONED PARQUET OUTPUT
    // --------------------------------------------------

    println("8. WRITE PARTITIONED PARQUET")

    repartitionedSales.write
      .mode("overwrite")
      .partitionBy("year", "month", "day")
      .parquet("data/day20/output/partitioned_sales")

    println(
      "Partitioned Parquet output written to: " +
        "data/day20/output/partitioned_sales"
    )

    // --------------------------------------------------
    // 9. READ PARTITIONED DATA
    // --------------------------------------------------

    println("9. READ PARTITIONED PARQUET")

    val partitionedSales = spark.read
      .parquet("data/day20/output/partitioned_sales")

    partitionedSales
      .orderBy("sale_date", "sale_id")
      .show(false)

    println(
      "Partitioned record count: " +
        partitionedSales.count()
    )

    // --------------------------------------------------
    // 10. FILE FORMAT COMPARISON
    // --------------------------------------------------

    println("10. FILE FORMAT COMPARISON")

    println("CSV:")
    println("- Human-readable tabular text format.")
    println("- Easy to exchange between applications.")
    println("- Larger storage size and less efficient for analytics.")

    println("JSON:")
    println("- Semi-structured and human-readable.")
    println("- Useful for APIs and nested data.")
    println("- Usually larger than columnar formats.")

    println("Parquet:")
    println("- Columnar storage format.")
    println("- Efficient for analytical workloads.")
    println("- Supports compression and efficient column access.")
    println("- Commonly used with Apache Spark.")

    // --------------------------------------------------
    // 11. PARTITIONED FILE LAYOUT
    // --------------------------------------------------

    println("11. PARTITIONED FILE LAYOUT")

    println("Partition columns:")
    println("- year")
    println("- month")
    println("- day")

    println("Expected layout pattern:")
    println(
      "partitioned_sales/year=2026/month=9/day=1/part-*.parquet"
    )
    println(
      "partitioned_sales/year=2026/month=9/day=2/part-*.parquet"
    )
    println(
      "partitioned_sales/year=2026/month=9/day=3/part-*.parquet"
    )
    println("...")
    println(
      "partitioned_sales/year=2026/month=9/day=7/part-*.parquet"
    )

    println("Partitioning allows Spark to skip irrelevant partitions")
    println("when filters are applied on partition columns.")

    // --------------------------------------------------
    // 12. NUMBER OF OUTPUT FILES
    // --------------------------------------------------

    println("12. OUTPUT FILE CONCEPT")

    println("- Spark writes data using partitions.")
    println("- Each Spark task can produce an output part file.")
    println("- repartition() changes the number of Spark partitions.")
    println("- The final number of files depends on Spark partitions")
    println("  and the partitioned output layout.")
    println("- Too many small files can create a small-file problem.")
    println("- Too few large files can reduce parallelism.")

    // --------------------------------------------------
    // 13. DAILY SALES SCENARIO
    // --------------------------------------------------

    println("13. DAILY SALES SCENARIO")

    println("- Read daily sales records from CSV.")
    println("- Transform the sale date into year/month/day.")
    println("- Store analytical data in Parquet.")
    println("- Partition output by year, month and day.")
    println("- Use repartition() to control write parallelism.")
    println("- Query only required date partitions when possible.")

    println("======================================")
    println("DAY 20 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
