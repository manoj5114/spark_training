Examples for "Intro to Apache Spark" training
===============
The examples cover the core concepts of Apache Spark
* Spark Core
  - RDDs
  - Transformations
  - Actions
  - Pair RDDs, Joins
* Spark SQL
* Spark Steaming

Provided are the following self-contained Spark apps written in Scala
* ScalaPrimer.scala (plain scala program)
* DistributedSort.scala
* CensusAnalyzer.scala
* CensusAnalyzerSQL.scala
* TwitterStudy.scala


These examples have been created to run against Spark 1.2 and may also be run against 1.1

Code provided is entirely in Scala wih some bits in SparkSQL.

Data used by the examples are provided in the data folder.

Requirements
* JDK 1.7 or higher
* Scala 2.10.4
* spark-core 1.2.0
* spark-sql 1.2.0
* SBT 0.13.8

Spark Submit
===

You can also create a jar with all of the dependencies for running scala versions of the code and run the job with the spark-submit script
* To compile the code to a jar
  cd spark_training
  ./sbt/sbt package

* To run each app individually, from the spark_training folder specifiy each programm class name indivudually, like:

$SPARK_HOME/bin/spark-submit --class CensusAnalyzer --master local target/scala-2.10/spark_training_2.10-0.1.jar

$SPARK_HOME/bin/spark-submit --class TwitterStudy --master local target/scala-2.10/spark_training_2.10-0.1.jar



