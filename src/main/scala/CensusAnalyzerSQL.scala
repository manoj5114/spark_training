/**
 * Created by snudurupati on 5/1/15.
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object CensusAnalyzerSQL {

  //Define two classes in order to create schema RDDs
  case class Population(code: Int, population: Int)
  case class County(code: Int, county: String)

   def main(args: Array[String]) {

     Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
     Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

     val conf = new SparkConf().setAppName("NaiveBayesClassifier").setMaster("local").set("spark.driver.host","localhost")
     val sc = new SparkContext(conf)
     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     import sqlContext.createSchemaRDD

     //1. Create two RDDs from the provided CSV files
     val popData = sc.textFile("data/pop_data.csv")
     val countyNames = sc.textFile("data/county_names.csv")

     //2. Split each line into two parts based on comma(“,") character and create schema RDD of each
     val popRDD = popData.map(s => s.split(",")).map(s => Population(s(0).toInt, s(1).toInt))
     val countyRDD = countyNames.map(s => s.split(",")).map(s => County(s(0).toInt, s(1)))

     //3. Register the schema RDDs as temp tables.
     popRDD.registerTempTable("population")
     countyRDD.registerTempTable("counties")

     //4. Join the two tables and register the resultset as a new table
     val censusRDD = sqlContext.sql("select p.code, c.county, p.population from population p join counties c on p.code = c.code")
     censusRDD.registerTempTable("census")
     sqlContext.cacheTable("census")

     //5. Now lets calculate the max, min and avg populations using SQL
     val popMetrics = sqlContext.sql("select min(population) as minpop,max(population) as maxpop, avg(population) as avgpop from census").collect
     popMetrics.foreach(r => println("Min: %d, Max: %d, Avg: %10.2f".format(r(0), r(1), r(2))))

     //6. Show the top 10 counties by population
     val top10Counties = sqlContext.sql("select county, population from census order by population desc limit 10").collect
     top10Counties.foreach(println)

   }
 }
