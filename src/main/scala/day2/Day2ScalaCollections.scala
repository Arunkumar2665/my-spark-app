package day2

object Day2ScalaCollections {

  // --------------------------------------------------
  // Case classes for Day 2 data
  // --------------------------------------------------

  case class Sale(
    product: String,
    quantity: Int,
    price: Double
  )

  case class Customer(
    id: Int,
    name: String
  )

  case class Order(
    orderId: Int,
    customerId: Int,
    product: String,
    quantity: Int
  )

  // --------------------------------------------------
  // 1. Process Sales List
  // map, filter, flatMap, reduce
  // --------------------------------------------------

  val sales = List(
    Sale("Laptop", 2, 50000),
    Sale("Mouse", 5, 800),
    Sale("Keyboard", 3, 1500),
    Sale("Monitor", 2, 12000),
    Sale("Headphones", 4, 2500)
  )

  // map - calculate total amount for every sale
  val saleTotals = sales.map { sale =>
    sale.product -> (sale.quantity * sale.price)
  }

  // filter - select sales where quantity >= 3
  val highQuantitySales = sales.filter(_.quantity >= 3)

  // flatMap - create one product entry for every unit sold
  val individualProducts = sales.flatMap { sale =>
    List.fill(sale.quantity)(sale.product)
  }

  // reduce - calculate total sales revenue
  val totalRevenue = sales
    .map(sale => sale.quantity * sale.price)
    .reduce(_ + _)

  // --------------------------------------------------
  // 2. Vector for Indexed Customer Records
  // --------------------------------------------------

  val customers = Vector(
    Customer(101, "Arun"),
    Customer(102, "Rahul"),
    Customer(103, "Priya"),
    Customer(104, "Sneha")
  )

  // Vector supports efficient indexed access
  val customerAtIndex2 = customers(2)

  // --------------------------------------------------
  // 3. Map for Product Quantities and Prices
  // --------------------------------------------------

  val productQuantities = Map(
    "Laptop" -> 2,
    "Mouse" -> 5,
    "Keyboard" -> 3,
    "Monitor" -> 2,
    "Headphones" -> 4
  )

  val productPrices = Map(
    "Laptop" -> 50000.0,
    "Mouse" -> 800.0,
    "Keyboard" -> 1500.0,
    "Monitor" -> 12000.0,
    "Headphones" -> 2500.0
  )

  // Calculate revenue for each product
  val productRevenue = productQuantities.map {
    case (product, quantity) =>
      val price = productPrices.getOrElse(product, 0.0)
      product -> (quantity * price)
  }

  // --------------------------------------------------
  // 4. For-comprehension combining Customers and Orders
  // --------------------------------------------------

  val orders = List(
    Order(1001, 101, "Laptop", 1),
    Order(1002, 102, "Mouse", 2),
    Order(1003, 101, "Keyboard", 1),
    Order(1004, 103, "Monitor", 1),
    Order(1005, 104, "Headphones", 2)
  )

  val customerOrders = for {
    customer <- customers
    order <- orders
    if customer.id == order.customerId
  } yield {
    customer.name -> order
  }

  // --------------------------------------------------
  // 5. Daily Sales Summary
  // --------------------------------------------------

  val dailySalesSummary = sales.map { sale =>
    val revenue = sale.quantity * sale.price

    s"${sale.product}: Quantity=${sale.quantity}, Revenue=₹$revenue"
  }

  // --------------------------------------------------
  // Main
  // --------------------------------------------------

  def main(args: Array[String]): Unit = {

    println("======================================")
    println("DAY 2 - SCALA COLLECTIONS PRACTICE")
    println("======================================")

    // ------------------------------------------------
    // Sales List
    // ------------------------------------------------

    println("\n1. SALES LIST")
    sales.foreach(println)

    // map
    println("\n2. MAP - TOTAL FOR EACH SALE")

    saleTotals.foreach {
      case (product, total) =>
        println(s"$product -> ₹$total")
    }

    // filter
    println("\n3. FILTER - QUANTITY >= 3")

    highQuantitySales.foreach(println)

    // flatMap
    println("\n4. FLATMAP - INDIVIDUAL PRODUCTS")

    println(individualProducts)

    // reduce
    println("\n5. REDUCE - TOTAL REVENUE")

    println(s"Total Revenue = ₹$totalRevenue")

    // ------------------------------------------------
    // Vector
    // ------------------------------------------------

    println("\n6. VECTOR - CUSTOMER RECORDS")

    customers.foreach(println)

    println(s"\nCustomer at index 2: $customerAtIndex2")

    println(
      "Vector is useful for indexed customer records because " +
      "it provides efficient indexed access while remaining immutable."
    )

    // ------------------------------------------------
    // Map
    // ------------------------------------------------

    println("\n7. MAP - PRODUCT QUANTITIES")

    productQuantities.foreach {
      case (product, quantity) =>
        println(s"$product -> Quantity: $quantity")
    }

    println("\n8. MAP - PRODUCT REVENUE")

    productRevenue.foreach {
      case (product, revenue) =>
        println(s"$product -> Revenue: ₹$revenue")
    }

    // ------------------------------------------------
    // For-comprehension
    // ------------------------------------------------

    println("\n9. FOR-COMPREHENSION - CUSTOMERS AND ORDERS")

    customerOrders.foreach {
      case (customerName, order) =>
        println(
          s"$customerName -> Order ${order.orderId}, " +
          s"${order.product}, Quantity: ${order.quantity}"
        )
    }

    // ------------------------------------------------
    // Daily Sales Summary
    // ------------------------------------------------

    println("\n10. DAILY SALES SUMMARY")

    dailySalesSummary.foreach(println)

    println("\n======================================")
    println("DAY 2 COMPLETED")
    println("======================================")
  }
}
