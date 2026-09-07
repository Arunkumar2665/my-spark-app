object Day1ScalaEssentials {

  // --------------------------------------------------
  // 1. val, var and lazy val
  // --------------------------------------------------

  val college = "ABC Engineering College"

  var studentCount = 5

  lazy val expensiveCalculation = {
    println("lazy val is being evaluated...")
    100 * 20
  }

  // --------------------------------------------------
  // 2. Immutable Collections
  // --------------------------------------------------

  val students = List(
    "Arun",
    "Rahul",
    "Priya",
    "Sneha",
    "Kiran"
  )

  val marks = Vector(85, 72, 91, 68, 78)

  val subjects = Set(
    "Scala",
    "Spark",
    "SQL"
  )

  val studentMarks = Map(
    "Arun" -> 85,
    "Rahul" -> 72,
    "Priya" -> 91,
    "Sneha" -> 68,
    "Kiran" -> 78
  )

  // --------------------------------------------------
  // 3. For-comprehension with yield
  // --------------------------------------------------

  val passedStudents = for {
    (name, mark) <- studentMarks
    if mark >= 70
  } yield name

  val studentGrades = for {
    (name, mark) <- studentMarks
  } yield {
    val grade =
      if (mark >= 90) "A+"
      else if (mark >= 80) "A"
      else if (mark >= 70) "B"
      else if (mark >= 60) "C"
      else "F"

    name -> grade
  }

  // --------------------------------------------------
  // 4. Logger Trait
  // --------------------------------------------------

  trait Logger {
    def log(message: String): Unit
  }

  class ConsoleLogger extends Logger {
    override def log(message: String): Unit = {
      println(s"[Console] $message")
    }
  }

  class StudentLogger extends Logger {
    override def log(message: String): Unit = {
      println(s"[Student] $message")
    }
  }

  // --------------------------------------------------
  // 5. Student Grade Processor
  // --------------------------------------------------

  case class Student(
    name: String,
    marks: Int
  )

  def calculateGrade(mark: Int): String = {
    if (mark >= 90) "A+"
    else if (mark >= 80) "A"
    else if (mark >= 70) "B"
    else if (mark >= 60) "C"
    else "F"
  }

  def processStudents(students: List[Student]): List[String] = {

    students.map { student =>
      val grade = calculateGrade(student.marks)

      s"${student.name} -> Marks: ${student.marks}, Grade: $grade"
    }
  }

  // --------------------------------------------------
  // Main
  // --------------------------------------------------

  def main(args: Array[String]): Unit = {

    println("======================================")
    println("DAY 1 - SCALA ESSENTIALS")
    println("======================================")

    // val
    println(s"\nCollege using val: $college")

    // var
    println(s"Initial student count: $studentCount")

    studentCount += 1

    println(s"Updated student count using var: $studentCount")

    // lazy val
    println("\nBefore accessing lazy val")

    println(s"Lazy calculation result: $expensiveCalculation")

    // List
    println("\nStudents List:")
    println(students)

    // Vector
    println("\nMarks Vector:")
    println(marks)

    // Set
    println("\nSubjects Set:")
    println(subjects)

    // Map
    println("\nStudent Marks Map:")
    studentMarks.foreach {
      case (name, mark) =>
        println(s"$name -> $mark")
    }

    // for + yield
    println("\nStudents with marks >= 70:")
    println(passedStudents)

    println("\nStudent Grades using for/yield:")
    studentGrades.foreach {
      case (name, grade) =>
        println(s"$name -> $grade")
    }

    // Trait implementations
    println("\nLogger Trait:")
    val consoleLogger = new ConsoleLogger
    val studentLogger = new StudentLogger

    consoleLogger.log("Scala Day 1 started")
    studentLogger.log("Processing student grades")

    // Student Grade Processor
    val studentRecords = List(
      Student("Arun", 85),
      Student("Rahul", 72),
      Student("Priya", 91),
      Student("Sneha", 68),
      Student("Kiran", 55)
    )

    println("\nStudent Grade Processor:")
    processStudents(studentRecords).foreach(println)

    println("\n======================================")
    println("DAY 1 COMPLETED")
    println("======================================")
  }
}
