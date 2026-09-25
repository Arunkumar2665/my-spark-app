package day17

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Day17WindowFunctions {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day17WindowFunctions")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    println("======================================")
    println("DAY 17 - WINDOW FUNCTIONS")
    println("======================================")

    // ------------------------------------------------
    // 1. READ STUDENT SCORES
    // ------------------------------------------------

    println("\n1. READ STUDENT SCORES")

    val studentsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/day17/student_scores.csv")

    studentsDF.show(false)

    // ------------------------------------------------
    // 2. WINDOW PARTITION BY COURSE
    // ------------------------------------------------

    println("\n2. WINDOW PARTITION BY COURSE")

    val courseWindow = Window
      .partitionBy("course")
      .orderBy(col("score").desc, col("student_id"))

    println("Students are ranked independently within each course.")

    // ------------------------------------------------
    // 3. ROW_NUMBER
    // ------------------------------------------------

    println("\n3. ROW_NUMBER")

    val withRowNumber = studentsDF
      .withColumn(
        "row_number",
        row_number().over(courseWindow)
      )
      .orderBy("course", "row_number")

    withRowNumber.show(false)

    // ------------------------------------------------
    // 4. RANK
    // ------------------------------------------------

    println("\n4. RANK")

    val withRank = studentsDF
      .withColumn(
        "rank",
        rank().over(courseWindow)
      )
      .orderBy("course", "rank", "student_id")

    withRank.show(false)

    // ------------------------------------------------
    // 5. DENSE_RANK
    // ------------------------------------------------

    println("\n5. DENSE_RANK")

    val withDenseRank = studentsDF
      .withColumn(
        "dense_rank",
        dense_rank().over(courseWindow)
      )
      .orderBy("course", "dense_rank", "student_id")

    withDenseRank.show(false)

    // ------------------------------------------------
    // 6. TOP 3 STUDENTS PER COURSE
    // ------------------------------------------------

    println("\n6. TOP 3 STUDENTS PER COURSE")

    val top3Students = withRank
      .filter(col("rank") <= 3)
      .orderBy("course", "rank", "student_id")

    top3Students.show(false)

    // ------------------------------------------------
    // 7. LAG AND LEAD
    // ------------------------------------------------

    println("\n7. LAG AND LEAD")

    val scoreWindow = Window
      .partitionBy("course")
      .orderBy(col("score").desc, col("student_id"))

    val lagLeadDF = studentsDF
      .withColumn(
        "previous_score",
        lag("score", 1).over(scoreWindow)
      )
      .withColumn(
        "next_score",
        lead("score", 1).over(scoreWindow)
      )
      .orderBy("course", "score", "student_id")

    lagLeadDF.show(false)

    // ------------------------------------------------
    // 8. LATEST RECORD PER CUSTOMER
    // ------------------------------------------------

    println("\n8. LATEST RECORD PER CUSTOMER")

    val customerPolicies = Seq(
      ("C101", "Basic", "2026-01-15"),
      ("C101", "Premium", "2026-06-20"),
      ("C102", "Basic", "2026-02-10"),
      ("C102", "Gold", "2026-08-05"),
      ("C103", "Silver", "2026-03-12"),
      ("C103", "Premium", "2026-07-25"),
      ("C104", "Basic", "2026-04-18")
    ).toDF("customer_id", "policy", "policy_date")

    customerPolicies.show(false)

    val customerWindow = Window
      .partitionBy("customer_id")
      .orderBy(col("policy_date").desc)

    val latestPolicy = customerPolicies
      .withColumn(
        "row_number",
        row_number().over(customerWindow)
      )
      .filter(col("row_number") === 1)
      .drop("row_number")
      .orderBy("customer_id")

    println("Latest policy for each customer:")

    latestPolicy.show(false)

    // ------------------------------------------------
    // 9. WINDOW FUNCTION CONCEPTS
    // ------------------------------------------------

    println("\n9. WINDOW FUNCTION CONCEPTS")

    println("- Window functions calculate values across related rows.")
    println("- partitionBy() divides rows into independent groups.")
    println("- orderBy() defines the ordering inside each partition.")
    println("- row_number() assigns a unique sequential number.")
    println("- rank() gives equal ranks to tied values and leaves gaps.")
    println("- dense_rank() gives equal ranks to tied values without gaps.")
    println("- lag() accesses a previous row.")
    println("- lead() accesses a following row.")

    // ------------------------------------------------
    // 10. SCENARIO SUMMARY
    // ------------------------------------------------

    println("\n10. WINDOW FUNCTION SCENARIOS")

    println("- Rank students independently within each course.")
    println("- Identify the top 3 students per course.")
    println("- Compare row_number, rank, and dense_rank.")
    println("- Compare previous and next scores using lag and lead.")
    println("- Find the latest policy record for each customer.")

    println("\n======================================")
    println("DAY 17 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
