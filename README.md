# Scala + Apache Spark 30-Day Daily Practice

A hands-on learning project covering **Scala programming and Apache Spark** through a structured 30-day daily practice problem set.

Each day is implemented separately so that the concepts, source code, input data, execution steps, and progress can be tracked independently.

---

# Project Objective

The objective of this project is to build practical knowledge of:

* Scala programming
* Scala collections
* Apache Spark
* Spark Core
* Spark SQL
* RDDs
* Transformations and actions
* Lazy evaluation
* Lineage and fault tolerance
* DAG and Spark execution
* Pair RDDs
* Partitions and parallelism
* Shuffle operations
* Spark execution stages
* Data aggregation
* Data processing using sample datasets

The project will be developed progressively from **Day 1 to Day 30**.

---

# Technologies Used

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

# Project Structure

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
│           ├── day5/
│           │   └── Day5TransformationsActions.scala
│           │
│           ├── day6/
│           │   └── Day6WordCount.scala
│           │
│           ├── day7/
│           │   └── Day7LineageFaultTolerance.scala
│           │
│           ├── day8/
│           │   └── Day8DAGExecution.scala
│           │
│           ├── day9/
│           │   └── Day9PairRDD.scala
│           │
│           └── day10/
│               └── Day10Partitioning.scala
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
│   ├── day6/
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

| Day    | Topic                                     | Status      |
| ------ | ----------------------------------------- | ----------- |
| Day 1  | Scala Essentials                          | ✅ Completed |
| Day 2  | Scala Collections                         | ✅ Completed |
| Day 3  | Spark Setup                               | ✅ Completed |
| Day 4  | RDD Creation                              | ✅ Completed |
| Day 5  | Transformations and Actions               | ✅ Completed |
| Day 6  | Word Count                                | ✅ Completed |
| Day 7  | Immutability, Lineage and Fault Tolerance | ✅ Completed |
| Day 8  | DAG and Spark Execution                   | ✅ Completed |
| Day 9  | Pair RDD                                  | ✅ Completed |
| Day 10 | Partitioning                              | ✅ Completed |
| Day 11 | Upcoming                                  | ⏳           |
| ...    | ...                                       | ⏳           |
| Day 30 | Upcoming                                  | ⏳           |

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

```text
src/main/scala/day3/Day3SparkSetup.scala
```

## Run Command

```bash
sbt "runMain day3.Day3SparkSetup"
```

The application was tested using local Spark configurations including:

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

The application reported:

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

## Implementation

```text
src/main/scala/day5/Day5TransformationsActions.scala
```

## Input Data

```text
data/day5/application.log
```

## Transformations and Actions

The Day 5 implementation demonstrated:

```text
RDD
 ↓
Transformations
 ↓
Actions
 ↓
Results
```

`distinct()` was also used to demonstrate a transformation that requires a shuffle.

## Log Analyzer

The application analyzed the sample log file and counted ERROR messages.

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

The sample log contains **10 total entries** and **4 ERROR messages**.

## Partition Information

```text
Spark master: local[2]
Default parallelism: 2
Numbers RDD partitions: 2
Log RDD partitions: 2
```

## Run Command

```bash
sbt "runMain day5.Day5TransformationsActions"
```

## Result

The Day 5 application completed successfully after executing the transformation, action, log-analysis, and partition exercises.

## Git Commit

```text
35ee820 Implement Day 5 transformations and actions
```

---

# Day 6 — Word Count

## Objective

Implement the classic **Word Count** problem using Spark RDD transformations and actions.

## Concepts Covered

* `flatMap`
* `map`
* `reduceByKey`
* Case-insensitive word counting
* Removing punctuation
* Filtering empty words
* Log word-frequency analysis
* Top 10 frequent words
* Shuffle
* Partitions

## Implementation

```text
src/main/scala/day6/Day6WordCount.scala
```

## Input Data

```text
data/day6/application.log
```

## Basic Word Count

The basic pipeline was:

```text
Text RDD
   ↓
flatMap
   ↓
Individual words
   ↓
map
   ↓
(word, 1)
   ↓
reduceByKey
   ↓
Word counts
```

## Important Operations

### flatMap

Splits each line into individual words.

### map

Converts every word into a key-value pair:

```text
(word, 1)
```

### reduceByKey

Adds the values belonging to the same word.

```text
(word, 1)
(word, 1)
     ↓
(word, total)
```

## Case-Insensitive Counting

Words were converted to lowercase before counting so that different cases are treated as the same word.

## Punctuation Handling

Punctuation was removed and empty words were filtered before aggregation.

## Application Log Analysis

The application log was processed to identify the most frequent words.

Top observed words included:

```text
20260916 -> 14
info     -> 7
error    -> 5
connection -> 3
application -> 3
```

## Partition Information

```text
Spark master: local[2]
Default parallelism: 2
Log RDD partitions: 2
Word count RDD partitions: 2
```

## Run Command

```bash
sbt "runMain day6.Day6WordCount"
```

## Result

The Day 6 Word Count application was compiled and executed successfully.

---

# Day 7 — Immutability, Lineage and Fault Tolerance

## Objective

Understand RDD immutability, lineage, lazy evaluation and Spark fault tolerance.

## Concepts Covered

* RDD immutability
* Lineage
* Transformation chains
* Lazy evaluation
* Fault tolerance
* Partition recomputation
* RDD reuse

## Implementation

```text
src/main/scala/day7/Day7LineageFaultTolerance.scala
```

## Transformation Chain

The following RDD chain was implemented:

```text
numbersRDD
    ↓
  filter
    ↓
 evenRDD
    ↓
   map
    ↓
squaredRDD
    ↓
  filter
    ↓
greaterThanTenRDD
```

## Results

Input:

```text
1, 2, 3, 4, 5, 6, 7, 8, 9, 10
```

Even values:

```text
2, 4, 6, 8, 10
```

Squared values:

```text
4, 16, 36, 64, 100
```

Values greater than 10:

```text
16, 36, 64, 100
```

## RDD Immutability

The original RDD remains unchanged.

Each transformation creates a new RDD:

```text
numbersRDD
    ↓
evenRDD
    ↓
squaredRDD
    ↓
greaterThanTenRDD
```

## Lineage

Spark maintains information about the transformations used to create an RDD.

The actual RDD debug information was inspected using:

```scala
greaterThanTenRDD.toDebugString
```

## Fault Tolerance

RDDs are fault tolerant because Spark stores lineage information.

Conceptually:

```text
Lost Partition
      ↓
Use Lineage Information
      ↓
Recompute Required Transformations
      ↓
Recover Partition
```

If a partition is lost, Spark can recompute that partition from the required lineage rather than requiring the complete dataset to be recreated.

## Run Command

```bash
sbt "runMain day7.Day7LineageFaultTolerance"
```

## Result

The Day 7 application was compiled and executed successfully.

---

# Day 8 — DAG and Spark Execution

## Objective

Understand Spark's DAG, jobs, stages, tasks, partitions and shuffle boundaries.

## Concepts Covered

* DAG
* Jobs
* Stages
* Tasks
* Partitions
* Narrow transformations
* Wide transformations
* Shuffle boundaries
* `reduceByKey`
* RDD lineage

## Implementation

```text
src/main/scala/day8/Day8DAGExecution.scala
```

## Narrow Transformations

The implementation used:

```text
filter
map
```

These are narrow transformations because data does not need to move between partitions for these operations.

## Wide Transformation

The implementation used:

```text
reduceByKey
```

`reduceByKey` is a wide transformation because it requires a shuffle.

## DAG

The logical pipeline was:

```text
Parallelize
     |
     v
  filter
     |
     v
    map
     |
     v
 reduceByKey
     |
   SHUFFLE
     |
     v
  collect
     |
     v
   Result
```

## Jobs, Stages, Tasks and Partitions

### Job

A Spark job is created when an action such as `collect()` is executed.

### Stage

A stage is a group of tasks separated by shuffle boundaries.

### Task

A task is a unit of work executed on one partition.

### Partition

A partition is a portion of distributed data processed by a task.

## Stage Prediction

For the pipeline:

```text
parallelize → filter → map → reduceByKey → collect
```

the expected stages are:

```text
Stage 0
parallelize → filter → map
        |
      SHUFFLE
        |
        v
Stage 1
reduceByKey → collect
```

This single `reduceByKey → collect` job has an expected **2-stage execution**.

## Observed Execution

Spark execution logs showed a:

```text
ShuffleMapStage
```

followed by a:

```text
ResultStage
```

confirming the shuffle boundary.

## Run Command

```bash
sbt "runMain day8.Day8DAGExecution"
```

## Result

The Day 8 DAG and Spark execution application was compiled and executed successfully.

---

# Day 9 — Pair RDD

## Objective

Work with key-value RDDs and understand aggregation using Pair RDD operations.

## Concepts Covered

* Pair RDDs
* Key-value data
* `reduceByKey`
* `groupByKey`
* `mapValues`
* Revenue aggregation
* Department aggregation
* Bank transaction aggregation
* Shuffle
* Aggregation performance

## Implementation

```text
src/main/scala/day9/Day9PairRDD.scala
```

## Product Revenue

Sample sales data was represented as:

```text
Laptop → 50000
Mobile → 20000
Laptop → 45000
Tablet → 30000
Mobile → 15000
```

Using `reduceByKey`, revenue was aggregated as:

```text
Laptop → 95000
Mobile → 35000
Tablet → 30000
```

## Department Revenue

The department aggregation produced:

```text
Clothing → 45000
Electronics → 80000
Groceries → 25000
```

## reduceByKey

`reduceByKey` aggregates values belonging to the same key and performs local aggregation before the shuffle.

This can reduce the amount of data transferred during the shuffle.

## groupByKey

`groupByKey` groups all values belonging to the same key.

It can result in more data being transferred during the shuffle and can require more memory.

## mapValues

`mapValues` modifies the values while preserving the keys.

## Bank Transaction Aggregation

Transactions were aggregated by account ID.

Results:

```text
ACC101 → 8500.00
ACC102 → 4500.00
ACC103 → 7000.00
```

## Run Command

```bash
sbt "runMain day9.Day9PairRDD"
```

## Result

The Day 9 Pair RDD application was compiled and executed successfully.

---

# Day 10 — Partitioning

## Objective

Understand Spark partitions and practice `repartition`, `coalesce`, and `partitionBy`.

## Concepts Covered

* Partition inspection
* Partition count
* `mapPartitionsWithIndex`
* `repartition`
* `coalesce`
* `partitionBy`
* `HashPartitioner`
* Parallelism
* Partition optimization

## Implementation

```text
src/main/scala/day10/Day10Partitioning.scala
```

## Inspecting Partitions

The base RDD was created from:

```text
1 to 20
```

The application inspected partition contents using:

```scala
mapPartitionsWithIndex
```

## Repartition

The RDD was changed to:

```text
repartition(4)
```

Result:

```text
Partitions after repartition(4): 4
```

`repartition()` performs a shuffle and can be used to increase or decrease the number of partitions.

## Coalesce

The RDD was then reduced using:

```text
coalesce(2)
```

Result:

```text
Partitions after coalesce(2): 2
```

`coalesce()` is commonly used to reduce partitions and can avoid a full shuffle when decreasing the partition count.

## partitionBy

A Pair RDD containing account transactions was partitioned using:

```scala
new HashPartitioner(3)
```

Result:

```text
Partitions after partitionBy(HashPartitioner(3)): 3
```

`HashPartitioner` uses the key's hash value to determine the partition.

## Repartition vs Coalesce

### repartition

* Changes partition count using a shuffle.
* Can increase or decrease partitions.
* Useful when data needs redistribution.

### coalesce

* Primarily used to decrease partitions.
* Usually involves less data movement.
* Useful after filtering or reducing data volume.

## When to Increase Partitions

Increase partitions when:

* Dataset is large.
* Existing partitions are too large.
* More parallelism is needed.
* Some tasks are taking too long.

## When to Decrease Partitions

Decrease partitions when:

* Dataset becomes smaller after filtering.
* Too many small partitions create overhead.
* Fewer output files are required.

## Optimization Scenario

If a dataset has too few partitions:

```text
Dataset
   ↓
repartition()
   ↓
More partitions
   ↓
Better parallel processing
```

If a dataset becomes small after filtering:

```text
Large Dataset
     ↓
   filter
     ↓
Small Dataset
     ↓
 coalesce()
     ↓
Fewer unnecessary partitions
```

## Run Command

```bash
sbt "runMain day10.Day10Partitioning"
```

## Result

The Day 10 partitioning application was compiled and executed successfully.

---

# Important Spark Concepts Learned So Far

By the end of Day 10, the project has covered:

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
Lineage
  ↓
Fault Tolerance
  ↓
DAG
  ↓
Shuffle
  ↓
Stages
  ↓
Tasks
  ↓
Pair RDD
  ↓
Partitioning
```

## Key Transformations

```text
map
filter
flatMap
distinct
union
reduceByKey
groupByKey
mapValues
repartition
coalesce
partitionBy
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

### Day 6

```bash
sbt "runMain day6.Day6WordCount"
```

### Day 7

```bash
sbt "runMain day7.Day7LineageFaultTolerance"
```

### Day 8

```bash
sbt "runMain day8.Day8DAGExecution"
```

### Day 9

```bash
sbt "runMain day9.Day9PairRDD"
```

### Day 10

```bash
sbt "runMain day10.Day10Partitioning"
```

---

# Git Workflow

Days 1–5 were previously committed individually.

Days 6–10 were completed as a batch and will be committed and pushed together with the updated README.

Current history:

```text
Day 1 → 1f4cad7
Day 2 → bba181d
Day 3 → 21d336d
Day 4 → a3658c7
Day 5 → 35ee820
Day 6–10 → New batch commit
```

The workflow for the current batch is:

```text
Complete Days 6–10
        ↓
Update README
        ↓
Verify files
        ↓
git add
        ↓
git commit
        ↓
git push
        ↓
Verify GitHub status
```

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
git commit -m "Implement Days 6-10 and update README"
```

Push:

```bash
git push origin main
```

Verify:

```bash
git status
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
4. Compile the application
        ↓
5. Run the application
        ↓
6. Verify the output
        ↓
7. Understand Spark execution
        ↓
8. Check partitions / shuffle / performance
        ↓
9. Update documentation
        ↓
10. Commit and push
```

---

# Future Plan

Completed:

```text
Day 1  ✅
Day 2  ✅
Day 3  ✅
Day 4  ✅
Day 5  ✅
Day 6  ✅
Day 7  ✅
Day 8  ✅
Day 9  ✅
Day 10 ✅
```

Upcoming:

```text
Day 11 ⏳
Day 12 ⏳
Day 13 ⏳
...
Day 30 ⏳
```

Each new day will continue to have:

* Separate source folder
* Separate input data where required
* Separate execution command
* Tested output
* Concept explanation
* Performance observations
* Git tracking

---

# Author

**Arun Kumar**

Scala + Apache Spark Learning Project

---

# Repository Status

**Progress: 10 / 30 Days Completed**

```text
██████████░░░░░░░░░░░░░░░░░░  33.3%
```

More Scala and Apache Spark concepts will be added as the 30-day practice progresses.
