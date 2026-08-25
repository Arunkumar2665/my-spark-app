import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SalesAnalytics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Sales Analytics")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // ==========================================
    // 1. READ SALES DATA
    // ==========================================

    val salesDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/sales.csv")

    println("=== Sales Data ===")
    salesDf.show()

    // ==========================================
    // 2. TOTAL SALES
    // ==========================================

    val salesWithAmount = salesDf.withColumn(
      "sales_amount",
      $"quantity" * $"price"
    )

    println("=== Sales With Amount ===")
    salesWithAmount.show()

    // ==========================================
    // 3. TOTAL SALES REVENUE
    // ==========================================

    val totalRevenue = salesWithAmount
      .agg(sum("sales_amount").alias("total_revenue"))

    println("=== Total Revenue ===")
    totalRevenue.show()

    // ==========================================
    // 4. REVENUE BY REGION
    // ==========================================

    val revenueByRegion = salesWithAmount
      .groupBy("region")
      .agg(
        sum("sales_amount").alias("total_revenue"),
        sum("quantity").alias("total_quantity")
      )
      .orderBy(desc("total_revenue"))

    println("=== Revenue by Region ===")
    revenueByRegion.show()

    // ==========================================
    // 5. REVENUE BY CATEGORY
    // ==========================================

    val revenueByCategory = salesWithAmount
      .groupBy("category")
      .agg(
        sum("sales_amount").alias("total_revenue"),
        sum("quantity").alias("total_quantity")
      )
      .orderBy(desc("total_revenue"))

    println("=== Revenue by Category ===")
    revenueByCategory.show()

    // ==========================================
    // 6. TOP PRODUCTS
    // ==========================================

    val topProducts = salesWithAmount
      .groupBy("product")
      .agg(
        sum("sales_amount").alias("total_revenue"),
        sum("quantity").alias("total_quantity")
      )
      .orderBy(desc("total_revenue"))

    println("=== Top Products ===")
    topProducts.show()

    // ==========================================
    // 7. HIGH VALUE ORDERS
    // ==========================================

    val highValueOrders = salesWithAmount
      .filter($"sales_amount" > 100000)

    println("=== High Value Orders ===")
    highValueOrders.show()

    // ==========================================
    // 8. SPARK SQL
    // ==========================================

    salesWithAmount.createOrReplaceTempView("sales")

    val sqlAnalysis = spark.sql("""
      SELECT
        category,
        SUM(sales_amount) AS total_revenue,
        AVG(sales_amount) AS average_order_value
      FROM sales
      GROUP BY category
      ORDER BY total_revenue DESC
    """)

    println("=== Spark SQL Sales Analysis ===")
    sqlAnalysis.show()

    // ==========================================
    // 9. WRITE CSV OUTPUT
    // ==========================================

    println("=== Writing Region Analysis to CSV ===")

    revenueByRegion
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/sales_by_region")

    // ==========================================
    // 10. WRITE PARQUET OUTPUT
    // ==========================================

    println("=== Writing Sales Data to Parquet ===")

    salesWithAmount
      .write
      .mode("overwrite")
      .parquet("output/sales_parquet")

    spark.stop()
  }
}
