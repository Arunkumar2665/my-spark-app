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
     
    println("=== Department Data ===")

val deptDf = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/departments.csv")

deptDf.show()

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
    
    println("=== Aggregate Salary Statistics ===")

val aggregateStats = df.agg(
  max("salary").alias("maximum_salary"),
  min("salary").alias("minimum_salary"),
  avg("salary").alias("average_salary"),
  sum("salary").alias("total_salary")
)

aggregateStats.show()

    println("=== Employees with Salary > 80000 ===")

    val highSalary = df.filter($"salary" > 80000)
     
    highSalary.show()
    
    println("=== Salary Categories ===")

    val categorized = df.withColumn(
    "salary_category",
    when($"salary" >= 85000, "High")
      .otherwise("Medium")
    )

    categorized.show()
    println("=== Employees Sorted by Salary ===")

    val sorted = df.orderBy($"salary".desc)

    sorted.show()

    println("=== Employee Department Join ===")

val joinedDf = df
  .join(deptDf, Seq("dept"), "inner")

joinedDf.show()

    
    println("=== Spark SQL Query ===")

df.createOrReplaceTempView("employees")

val sqlResult = spark.sql("""
  SELECT dept, AVG(salary) AS average_salary
  FROM employees
  GROUP BY dept
  ORDER BY average_salary DESC
""")

sqlResult.show()

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
     
     println("=== Writing Employee Data to Parquet ===")

df.write
  .mode("overwrite")
  .parquet("output/employees_parquet")

    spark.stop()
  }
}
