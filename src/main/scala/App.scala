import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object App {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CSV Spark App")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/employees.csv")

    println("=== Employee Data ===")
    df.show()
    println("=== Total Number of Employees ===")

    val totalEmployees = df.count()

    println(s"Total Employees: $totalEmployees")

    println("=== Salary Statistics ===")

    val salaryStats = df.select(
    max("salary").alias("maximum_salary"),
    min("salary").alias("minimum_salary")
    )

    salaryStats.show()

    println("=== Employees with Salary > 80000 ===")

    val highSalary = df.filter($"salary" > 80000)

    highSalary.show()
    println("=== Employees Sorted by Salary ===")

    val sorted = df.orderBy($"salary".desc)

    sorted.show()
    println("=== Average Salary by Department ===")

    val avgByDept = df
      .groupBy("dept")
      .avg("salary")
      .withColumnRenamed("avg(salary)", "average_salary")

    avgByDept.show()
    println("=== Writing Average Salary to CSV ===")

    avgByDept
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv("output/average_salary")

    spark.stop()
  }
}
