# Scala + Apache Spark 30-Day Daily Practice

A hands-on learning project covering **Scala programming and Apache Spark** through a structured 30-day daily practice problem set.

Each day is implemented separately so that the concepts, source code, input data, execution steps, and Git history can be tracked independently.

---

## Project Objective

The objective of this project is to build practical knowledge of:

* Scala programming
* Scala collections
* Apache Spark
* Spark Core
* Spark SQL
* RDDs
* Transformations and actions
* Lazy evaluation
* Partitions and parallelism
* Shuffle operations
* Spark execution stages
* Data processing using real sample datasets

The project will be developed progressively from **Day 1 to Day 30**.

---

## Technologies Used

* **Scala:** 2.12.18
* **Apache Spark:** 3.5.3
* **Spark Core:** 3.5.3
* **Spark SQL:** 3.5.3
* **SBT:** 1.10.10
* **Java:** 17.0.20
* **Operating System:** Ubuntu on WSL2
* **Version Control:** Git
* **Repository:** GitHub

---

## Project Structure

```text
my-spark-app/
│
├── src/
│   └── main/
│       └── scala/
│           ├── App.scala
│           ├── SalesAnalytics.scala
│           │
│           ├── day1/
│           │   └── Day1ScalaEssentials.scala
│           │
│           ├── day2/
│           │   └── Day2ScalaCollections.scala
│           │
│           ├── day3/
│           │   └── Day3SparkSetup.scala
│           │
│           ├── day4/
│           │   └── Day4RDDCreation.scala
│           │
│           └── day5/
│               └── Day5TransformationsActions.scala
│
├── data/
│   ├── day3/
│   │   └── sample.txt
│   │
│   ├── day4/
│   │   └── customers.txt
│   │
│   ├── day5/
│   │   └── application.log
│   │
│   ├── departments.csv
│   ├── employees.csv
│   └── sales.csv
│
├── project/
├── output/
├── build.sbt
└── README.md
```

---

# Environment Setup

## 1. Check Java

```bash
java -version
```

Java 17 is used for this project.

## 2. Check Scala

```bash
scala -version
```

## 3. Check SBT

```bash
sbt --version
```

## 4. Go to the project

```bash
cd ~/my-spark-app
```

## 5. Compile the project

```bash
sbt compile
```

A successful compilation confirms that the Scala and Spark dependencies are correctly configured.

---

# Build Configuration

The project uses:

```text
Scala 2.12.18
Apache Spark 3.5.3
Spark Core
Spark SQL
Java 17
```

Java 17 module-access options are configured in `build.sbt` for compatibility between Spark/Kryo and the Java module system.

The project runs Spark applications using:

```text
local[2]
```

This means Spark runs locally using **2 CPU cores**.

---

# Daily Progress

| Day    | Topic                       | Status      |
| ------ | --------------------------- | ----------- |
| Day 1  | Scala Essentials            | ✅ Completed |
| Day 2  | Scala Collections           | ✅ Completed |
| Day 3  | Spark Setup                 | ✅ Completed |
| Day 4  | RDD Creation                | ✅ Completed |
| Day 5  | Transformations and Actions | ✅ Completed |
| Day 6  | Upcoming                    | ⏳           |
| Day 7  | Upcoming                    | ⏳           |
| ...    | ...                         | ⏳           |
| Day 30 | Upcoming                    | ⏳           |

---

# Day 1 — Scala Essentials

## Objective

Learn and practice the fundamental concepts of Scala programming.

## Concepts Covered

* Variables
* Data types
* Functions
* Conditional statements
* Loops
* Basic Scala syntax
* Collections fundamentals

## Implementation

Source file:

```text
src/main/scala/day1/Day1ScalaEssentials.scala
```

## Run Command

```bash
sbt "runMain day1.Day1ScalaEssentials"
```

## Result

The Day 1 Scala exercises were implemented and executed successfully.

## Git Commit

```text
1f4cad7 Implement Day 1 Scala essentials
```

---

# Day 2 — Scala Collections

## Objective

Practice Scala collections and commonly used collection operations.

## Concepts Covered

* List
* Set
* Map
* Collection operations
* `map`
* `filter`
* `reduce`
* Other collection transformations

## Implementation

Source file:

```text
src/main/scala/day2/Day2ScalaCollections.scala
```

## Run Command

```bash
sbt "runMain day2.Day2ScalaCollections"
```

## Result

The Day 2 Scala collection exercises were implemented and executed successfully.

## Git Commit

```text
bba181d Implement Day 2 Scala collections
```

---

# Day 3 — Apache Spark Setup

## Objective

Set up Apache Spark and understand the basic Spark execution environment.

## Concepts Covered

* SparkSession
* SparkContext
* Local Spark execution
* Reading text files
* Spark configuration
* Partitions
* Default parallelism

## Input Data

```text
data/day3/sample.txt
```

## Implementation

Source file:

```text
src/main/scala/day3/Day3SparkSetup.scala
```

## Run Command

```bash
sbt "runMain day3.Day3SparkSetup"
```

The application was tested using different local configurations, including:

```text
local[2]
local[4]
```

## Important Concepts

### SparkSession

`SparkSession` is the main entry point for working with Spark applications, especially Spark SQL.

### SparkContext

`SparkContext` provides the connection to the Spark execution environment and is used for lower-level Spark operations such as RDD processing.

### Local Mode

```text
local[2]
```

runs Spark locally using two CPU cores.

## Result

The Spark environment was configured successfully and tested with text-file processing.

## Git Commit

```text
21d336d Implement Day 3 Spark setup
```

---

# Day 4 — RDD Creation

## Objective

Learn how to create and process **RDDs (Resilient Distributed Datasets)**.

## Concepts Covered

* Creating RDDs from Scala collections
* Creating RDDs from text files
* `map`
* `filter`
* `flatMap`
* Counting records
* Data filtering
* Partition information
* Default parallelism
* Local Spark execution

## Input Data

```text
data/day4/customers.txt
```

Example structure:

```text
101,Arun,Hyderabad
102,Ravi,Bangalore
103,Meena,Chennai
...
```

## Implementation

Source file:

```text
src/main/scala/day4/Day4RDDCreation.scala
```

## Run Command

```bash
sbt "runMain day4.Day4RDDCreation"
```

## Processing Flow

```text
Input Collection / File
        ↓
      RDD
        ↓
   Transformations
        ↓
      Actions
        ↓
     Results
```

## RDD Operations

### map

Used to transform each element.

```text
RDD → transformed RDD
```

### filter

Used to select elements that satisfy a condition.

```text
RDD → filtered RDD
```

### flatMap

Used to transform each input element into zero or more output elements and flatten the results.

## Partitioning

The customer RDD was processed using:

```text
local[2]
```

and the application reported:

```text
Default parallelism: 2
Customer file partitions: 2
```

## Result

The Day 4 RDD exercises were successfully implemented and executed.

## Git Commit

```text
a3658c7 Implement Day 4 RDD creation
```

---

# Day 5 — Transformations and Actions

## Objective

Understand the difference between **Spark transformations and actions** and practice commonly used RDD operations.

## Concepts Covered

### Transformations

* `map`
* `filter`
* `flatMap`
* `distinct`
* `union`

### Actions

* `count`
* `collect`
* `first`
* `take`
* `reduce`

### Additional Concepts

* Lazy evaluation
* Partitions
* Parallelism
* Shuffle
* Stages
* Log analysis

---

## Implementation

Source file:

```text
src/main/scala/day5/Day5TransformationsActions.scala
```

## Input Data

```text
data/day5/application.log
```

---

## Step 1 — Create RDDs

Two RDDs were created from Scala collections.

```text
RDD 1: 1, 2, 3, 4, 5
RDD 2: 4, 5, 6, 7, 8
```

---

## Step 2 — map Transformation

The `map` transformation was used to square each number.

```text
Input:
1, 2, 3, 4, 5

Output:
1, 4, 9, 16, 25
```

---

## Step 3 — filter Transformation

The `filter` transformation was used to select even numbers.

```text
Input:
1, 2, 3, 4, 5

Output:
2, 4
```

---

## Step 4 — flatMap Transformation

Two sentences were converted into individual words.

```text
Spark is fast
Scala works with Spark
```

Result:

```text
Spark, is, fast, Scala, works, with, Spark
```

---

## Step 5 — distinct Transformation

`distinct()` removes duplicate values.

Result:

```text
4, 5, 6, 7, 8
```

`distinct()` is an important Spark operation because it requires a **shuffle**.

During execution, Spark created a `ShuffleMapStage` followed by a `ResultStage`, demonstrating the shuffle involved in `distinct()`.

---

## Step 6 — union Transformation

`union()` combines two RDDs.

Result:

```text
1, 2, 3, 4, 5, 4, 5, 6, 7, 8
```

`union()` does not remove duplicates.

---

# Transformations vs Actions

## Transformations

Transformations create a new RDD from an existing RDD.

```text
map
filter
flatMap
distinct
union
```

Transformations are **lazy**.

They are not immediately executed when they are defined.

---

## Actions

Actions trigger Spark execution and return a result.

```text
count
collect
first
take
reduce
```

For example:

```text
Transformation
      ↓
Transformation
      ↓
    Action
      ↓
Spark executes the required computation
```

---

# Day 5 Actions

The following actions were tested:

### count

```text
Count: 5
```

### first

```text
First value: 1
```

### take

```text
First three values: 1, 2, 3
```

### collect

```text
Collect: 1, 2, 3, 4, 5
```

### reduce

```text
Reduce sum: 15
```

---

# Day 5 — Log Analyzer

A simple log analyzer was implemented using Spark RDD operations.

## Input

```text
data/day5/application.log
```

The log contains:

* INFO messages
* WARN messages
* ERROR messages

## Processing

```text
application.log
       ↓
   textFile()
       ↓
      RDD
       ↓
    filter()
       ↓
  ERROR messages
       ↓
    count()
       ↓
 ERROR count
```

The program counts all log entries and filters entries containing:

```text
ERROR
```

The sample log contains **10 total entries** and **4 ERROR messages**.

---

# Day 5 — Partition Information

The Spark application was executed using:

```text
Spark master: local[2]
```

The observed configuration was:

```text
Default parallelism: 2
Numbers RDD partitions: 2
Log RDD partitions: 2
```

This demonstrates the relationship between the local execution configuration and the number of partitions used by these RDDs.

---

# Day 5 — Spark Execution Concepts

## Lazy Evaluation

Spark transformations are lazy.

For example:

```scala
val squaredRDD = numbersRDD.map(number => number * number)
```

The `map` operation defines the transformation, but Spark executes it when an action such as:

```scala
squaredRDD.collect()
```

is called.

---

## Shuffle

A shuffle redistributes data between partitions.

`distinct()` is an example of an operation that requires a shuffle.

```text
RDD
 ↓
distinct()
 ↓
Shuffle
 ↓
ShuffleMapStage
 ↓
ResultStage
```

This was visible in the Spark execution logs during Day 5.

---

## Partitions

An RDD is divided into partitions so that Spark can process different portions of the data in parallel.

For this project:

```text
local[2]
     ↓
2 processing cores
     ↓
RDD partitions
```

---

# Day 5 Run Command

```bash
sbt "runMain day5.Day5TransformationsActions"
```

## Completion

The Day 5 application completed successfully after executing the transformation, action, log-analysis, and partition exercises.

---

# Important Spark Concepts Learned So Far

By the end of Day 5, the project has covered:

```text
Scala
  ↓
SparkSession
  ↓
SparkContext
  ↓
RDD
  ↓
Partitions
  ↓
Transformations
  ↓
Actions
  ↓
Lazy Evaluation
  ↓
Shuffle
  ↓
Stages
  ↓
Tasks
```

## Key Transformations

```text
map
filter
flatMap
distinct
union
```

## Key Actions

```text
count
collect
first
take
reduce
```

---

# Running the Completed Days

From the project root:

```bash
cd ~/my-spark-app
```

### Day 1

```bash
sbt "runMain day1.Day1ScalaEssentials"
```

### Day 2

```bash
sbt "runMain day2.Day2ScalaCollections"
```

### Day 3

```bash
sbt "runMain day3.Day3SparkSetup"
```

### Day 4

```bash
sbt "runMain day4.Day4RDDCreation"
```

### Day 5

```bash
sbt "runMain day5.Day5TransformationsActions"
```

---

# Git Workflow

Each completed day is maintained as a separate Git commit.

```text
Day 1 → Commit → Push
Day 2 → Commit → Push
Day 3 → Commit → Push
Day 4 → Commit → Push
Day 5 → Commit → Push
Day 6 → Commit → Push
...
Day 30 → Commit → Push
```

## Current Commit History

| Day   | Commit                                                  |
| ----- | ------------------------------------------------------- |
| Day 1 | `1f4cad7` — Implement Day 1 Scala essentials            |
| Day 2 | `bba181d` — Implement Day 2 Scala collections           |
| Day 3 | `21d336d` — Implement Day 3 Spark setup                 |
| Day 4 | `a3658c7` — Implement Day 4 RDD creation                |
| Day 5 | `35ee820` — Implement Day 5 transformations and actions |

---

# Git Commands Used

Check status:

```bash
git status
```

Stage changes:

```bash
git add .
```

Commit:

```bash
git commit -m "Implement Day X ..."
```

Push:

```bash
git push -u origin main
```

---

# Learning Approach

Each day follows the same workflow:

```text
1. Understand the problem
        ↓
2. Create the required Scala/Spark code
        ↓
3. Create sample input data
        ↓
4. Run the application
        ↓
5. Verify the output
        ↓
6. Understand Spark execution
        ↓
7. Check partitions / shuffle / performance
        ↓
8. Commit the work
        ↓
9. Push to GitHub
```

---

# Future Plan

The project will continue with the same structure for the remaining days.

```text
Day 1  ✅
Day 2  ✅
Day 3  ✅
Day 4  ✅
Day 5  ✅
Day 6  ⏳
Day 7  ⏳
...
Day 30 ⏳
```

Each new day will have:

* Separate source folder
* Separate input data where required
* Separate execution command
* Tested output
* Concept explanation
* Performance observations
* Separate Git commit
* GitHub push

---

# Author

**Arun Kumar**

Scala + Apache Spark Learning Project

---

# Repository Status

**Progress: 5 / 30 Days Completed**

```text
█████░░░░░░░░░░░░░░░░░░░░░  16.7%
```

More Spark and Scala concepts will be added as the 30-day practice progresses.

