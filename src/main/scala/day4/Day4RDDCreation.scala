package day4

import org.apache.spark.sql.SparkSession

object Day4RDDCreation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day4RDDCreation")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    println("======================================")
    println("DAY 4 - RDD CREATION")
    println("======================================")

    // 1. Create RDD from a Scala collection
    println("\n1. RDD FROM SCALA COLLECTION")

    val numbers = List(10, 20, 30, 40, 50)
    val numberRDD = sc.parallelize(numbers)

    println(s"Original collection: ${numbers.mkString(", ")}")
    println(s"RDD values: ${numberRDD.collect().mkString(", ")}")
    println(s"RDD partitions: ${numberRDD.getNumPartitions}")

    // 2. Create RDD from a text file
    println("\n2. RDD FROM TEXT FILE")

    val customerRDD = sc.textFile("data/day4/customers.txt")

    println(s"Number of customer records: ${customerRDD.count()}")
    println(s"Customer RDD partitions: ${customerRDD.getNumPartitions}")

    println("\nCustomer records:")
    customerRDD.collect().foreach(println)

    // 3. map transformation
    println("\n3. MAP TRANSFORMATION")

    val customerNames = customerRDD.map { line =>
      line.split(",")(1)
    }

    println(s"Customer names: ${customerNames.collect().mkString(", ")}")

    // 4. filter transformation
    println("\n4. FILTER TRANSFORMATION")

    val hyderabadCustomers = customerRDD.filter { line =>
      line.split(",")(2) == "Hyderabad"
    }

    println("Customers from Hyderabad:")
    hyderabadCustomers.collect().foreach(println)

    // 5. flatMap transformation
    println("\n5. FLATMAP TRANSFORMATION")

    val wordsRDD = customerRDD.flatMap { line =>
      line.split(",")
    }

    println("Values generated using flatMap:")
    println(wordsRDD.collect().mkString(", "))

    // 6. Total sales example using RDD
    println("\n6. TOTAL SALES USING RDD")

    val sales = List(1000.0, 2500.0, 1500.0, 3000.0, 2000.0)
    val salesRDD = sc.parallelize(sales)

    val totalSales = salesRDD.reduce(_ + _)

    println(s"Sales values: ${sales.mkString(", ")}")
    println(f"Total sales: ₹$totalSales%.2f")

    // 7. Default parallelism
    println("\n7. PARTITION AND DEFAULT PARALLELISM")

    println(s"Spark master: ${sc.master}")
    println(s"Default parallelism: ${sc.defaultParallelism}")
    println(s"Number RDD partitions: ${numberRDD.getNumPartitions}")
    println(s"Customer file partitions: ${customerRDD.getNumPartitions}")

    // 8. Larger customer file concept
    println("\n8. PARTITIONING CUSTOMER DATA")

    println(
      "Spark splits input data into partitions so that different tasks can process partitions in parallel."
    )

    println(
      "More partitions can allow better parallelism when sufficient CPU resources are available."
    )

    println("\n======================================")
    println("DAY 4 COMPLETED")
    println("======================================")

    spark.stop()
  }
}
