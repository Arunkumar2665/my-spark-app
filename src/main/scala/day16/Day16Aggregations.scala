package day16

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day16Aggregations {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day16Aggregations")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    println("======================================")
    println("DAY 16 - AGGREGATIONS")
    println("======================================")

    // ------------------------------------------------
    // 1. READ EMPLOYEE PAYROLL DATA
    // ------------------------------------------------

    println("\n1. READ EMPLOYEE PAYROLL DATA")

    val employeesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/day16/employee_payroll.csv")

    employeesDF.show()

    // ------------------------------------------------
    // 2. BASIC AGGREGATIONS
    // ------------------------------------------------

    println("\n2. BASIC AGGREGATIONS")

    val basicStats = employeesDF.agg(
      count("*").alias("employee_count"),
      sum("salary").alias("total_salary"),
      avg("salary").alias("average_salary"),
      min("salary").alias("minimum_salary"),
      max("salary").alias("maximum_salary")
    )

    basicStats.show()

    // ------------------------------------------------
    // 3. DEPARTMENT-WISE SALARY STATISTICS
    // ------------------------------------------------

    println("\n3. DEPARTMENT-WISE SALARY STATISTICS")

    val departmentStats = employeesDF
      .groupBy("department")
      .agg(
        count("*").alias("employee_count"),
        sum("salary").alias("total_salary"),
        round(avg("salary"), 2).alias("average_salary"),
        min("salary").alias("minimum_salary"),
        max("salary").alias("maximum_salary")
      )
      .orderBy("department")

    departmentStats.show(false)

    // ------------------------------------------------
    // 4. GROUP BY MULTIPLE COLUMNS
    // ------------------------------------------------

    println("\n4. GROUP BY DEPARTMENT AND LOCATION")

    val departmentLocationStats = employeesDF
      .groupBy("department", "location")
      .agg(
        count("*").alias("employee_count"),
        sum("salary").alias("total_salary"),
        round(avg("salary"), 2).alias("average_salary")
      )
      .orderBy("department", "location")

    departmentLocationStats.show(false)

    // ------------------------------------------------
    // 5. HAVING-LIKE FILTERING
    // ------------------------------------------------

    println("\n5. HAVING-LIKE FILTERING")

    println("Departments with total salary >= 200000:")

    val highSalaryDepartments = employeesDF
      .groupBy("department")
      .agg(
        count("*").alias("employee_count"),
        sum("salary").alias("total_salary"),
        round(avg("salary"), 2).alias("average_salary")
      )
      .filter($"total_salary" >= 200000)
      .orderBy(desc("total_salary"))

    highSalaryDepartments.show(false)

    // ------------------------------------------------
    // 6. AVERAGE SALARY FILTER
    // ------------------------------------------------

    println("\n6. DEPARTMENTS WITH AVERAGE SALARY >= 75000")

    val highAverageDepartments = employeesDF
      .groupBy("department")
      .agg(
        round(avg("salary"), 2).alias("average_salary")
      )
      .filter($"average_salary" >= 75000)
      .orderBy(desc("average_salary"))

    highAverageDepartments.show(false)

    // ------------------------------------------------
    // 7. HOSPITAL DEPARTMENT REVENUE SCENARIO
    // ------------------------------------------------

    println("\n7. HOSPITAL DEPARTMENT REVENUE SCENARIO")

    val hospitalData = Seq(
      ("H001", "Cardiology", 450000.0),
      ("H002", "Cardiology", 320000.0),
      ("H003", "Neurology", 500000.0),
      ("H004", "Neurology", 275000.0),
      ("H005", "Orthopedics", 380000.0),
      ("H006", "Orthopedics", 420000.0),
      ("H007", "Pediatrics", 210000.0),
      ("H008", "Pediatrics", 190000.0)
    ).toDF("patient_id", "department", "revenue")

    hospitalData.show()

    val hospitalRevenue = hospitalData
      .groupBy("department")
      .agg(
        count("*").alias("patient_count"),
        sum("revenue").alias("total_revenue"),
        round(avg("revenue"), 2).alias("average_revenue"),
        min("revenue").alias("minimum_revenue"),
        max("revenue").alias("maximum_revenue")
      )
      .orderBy(desc("total_revenue"))

    println("Hospital department revenue metrics:")

    hospitalRevenue.show(false)

    // ------------------------------------------------
    // 8. CONCEPT SUMMARY
    // ------------------------------------------------

    println("\n8. AGGREGATION CONCEPT SUMMARY")

    println("- count() counts records.")
    println("- sum() calculates the total value.")
    println("- avg() calculates the average value.")
    println("- min() finds the minimum value.")
    println("- max() finds the maximum value.")

    println("\n- groupBy() creates groups before aggregation.")
    println("- Multiple columns can be used with groupBy().")
    println("- Filtering aggregated results is similar to SQL HAVING.")
    println("- Spark aggregations are useful for analytical reports.")

    println("\n9. EMPLOYEE PAYROLL SCENARIO")

    println("- Calculate department-wise employee counts.")
    println("- Calculate total and average salaries.")
    println("- Identify minimum and maximum salaries.")
    println("- Filter departments using aggregated salary values.")

    println("\n======================================")
    println("DAY 16 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
