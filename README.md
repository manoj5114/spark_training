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


These examples have been created to run against Spark 1.2 and may also be run against 1.1

Code provided is entirely in Scala wih some bits in SparkSQL.

Data used by the examples are provided in the data folder.

Requirements
* JDK 1.7 or higher
* Scala 2.10.4
* spark-core 1.2.0
* spark-sql 1.2.0

Spark Submit
===

You can also create an jar with all of the dependencies for running in scala versions of the code and run the job with the spark-submit script

./sbt/sbt package
cd $SPARK_HOME; ./bin/spark-submit   

