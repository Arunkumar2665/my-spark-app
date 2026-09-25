package day14

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.Encoders

case class Employee(
  employee_id: String,
  name: String,
  department: String,
  salary: Double
)

object Day14DataFrameDataset {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day14DataFrameDataset")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    println("======================================")
    println("DAY 14 - DATAFRAME AND DATASET")
    println("======================================")

    // ------------------------------------------------
    // 1. CREATE DATAFRAME FROM CSV
    // ------------------------------------------------

    println("\n1. CREATE DATAFRAME FROM CSV")

    val employeeDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/day14/employees.csv")

    employeeDF.show(false)

    println("Employee DataFrame created successfully.")

    // ------------------------------------------------
    // 2. INSPECT DATAFRAME SCHEMA
    // ------------------------------------------------

    println("\n2. DATAFRAME SCHEMA")

    employeeDF.printSchema()

    // ------------------------------------------------
    // 3. CONVERT DATAFRAME TO DATASET
    // ------------------------------------------------

    println("\n3. DATAFRAME TO DATASET")

    val employeeDS: Dataset[Employee] =
      employeeDF.as[Employee]

    println("DataFrame successfully converted to Dataset[Employee].")

    employeeDS.show(false)

    // ------------------------------------------------
    // 4. TYPED DATASET OPERATION
    // ------------------------------------------------

    println("\n4. TYPED DATASET OPERATION")

    val highSalaryEmployees =
      employeeDS.filter(employee => employee.salary >= 80000)

    println("Employees with salary >= 80000:")

    highSalaryEmployees.show(false)

    println(
      s"High salary employee count: ${highSalaryEmployees.count()}"
    )

    // ------------------------------------------------
    // 5. TYPED DATASET MAP OPERATION
    // ------------------------------------------------

    println("\n5. TYPED DATASET MAP OPERATION")

    val employeeWithBonus =
      employeeDS.map { employee =>
        val bonus = employee.salary * 0.10

        (
          employee.employee_id,
          employee.name,
          employee.department,
          employee.salary,
          bonus
        )
      }

    println("Employee salary with 10% bonus:")

    employeeWithBonus.show(false)

    // ------------------------------------------------
    // 6. CONVERT DATASET BACK TO DATAFRAME
    // ------------------------------------------------

    println("\n6. DATASET TO DATAFRAME")

    val employeeBonusDF =
      employeeWithBonus.toDF(
        "employee_id",
        "name",
        "department",
        "salary",
        "bonus"
      )

    employeeBonusDF.show(false)

    println("Dataset successfully converted back to DataFrame.")

    // ------------------------------------------------
    // 7. EMPLOYEE PAYROLL REPORT
    // ------------------------------------------------

    println("\n7. EMPLOYEE PAYROLL REPORT")

    employeeBonusDF
      .select(
        "employee_id",
        "name",
        "department",
        "salary",
        "bonus"
      )
      .orderBy($"salary".desc)
      .show(false)

    // ------------------------------------------------
    // 8. DATAFRAME VS DATASET VS RDD
    // ------------------------------------------------

    println("\n8. RDD VS DATAFRAME VS DATASET")

    println("RDD:")
    println("- Low-level distributed collection.")
    println("- Provides strong control over transformations.")
    println("- Does not provide Spark SQL schema-based optimization.")

    println("\nDataFrame:")
    println("- Distributed collection organized into named columns.")
    println("- Provides schema-based operations.")
    println("- Works with Spark SQL and Catalyst optimization.")

    println("\nDataset:")
    println("- Combines DataFrame optimization with compile-time type safety.")
    println("- Uses case classes for strongly typed records.")
    println("- Supports typed transformations and functional operations.")

    // ------------------------------------------------
    // 9. TYPE SAFETY
    // ------------------------------------------------

    println("\n9. TYPE SAFETY")

    println("Dataset[Employee] provides compile-time type safety.")

    println("The Employee case class defines:")
    println("- employee_id: String")
    println("- name: String")
    println("- department: String")
    println("- salary: Double")

    println("Typed Dataset operations work with Employee objects.")

    // ------------------------------------------------
    // 10. CATALYST OPTIMIZATION
    // ------------------------------------------------

    println("\n10. CATALYST OPTIMIZATION")

    println("Spark SQL uses Catalyst to optimize DataFrame and Dataset")
    println("logical query plans before execution.")

    println("Examples include:")
    println("- Predicate pushdown")
    println("- Column pruning")
    println("- Logical and physical plan optimization")

    // ------------------------------------------------
    // 11. PAYROLL SCENARIO
    // ------------------------------------------------

    println("\n11. EMPLOYEE PAYROLL SCENARIO")

    println("- Load employee payroll data from CSV.")
    println("- Create a DataFrame.")
    println("- Convert DataFrame to Dataset[Employee].")
    println("- Apply typed salary filtering.")
    println("- Calculate employee bonuses.")
    println("- Convert Dataset back to DataFrame.")
    println("- Generate an employee payroll report.")

    // ------------------------------------------------
    // 12. ENCODER INFORMATION
    // ------------------------------------------------

    println("\n12. DATASET ENCODER")

    println(
      s"Employee encoder available: ${Encoders.product[Employee].schema}"
    )

    println("\n======================================")
    println("DAY 14 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
