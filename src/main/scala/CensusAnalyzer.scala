/**
 * Created by snudurupati on 5/1/15.
 * A an analysis of populations by counties to demonstrate joins between pair RDDs.
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object CensusAnalyzer {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("CensusAnalyzer").setMaster("local").set("spark.driver.host","localhost")
    val sc = new SparkContext(conf)

    //1. Create two RDDs from the provided CSV files
    val popData = sc.textFile("data/pop_data.csv")
    val countyNames = sc.textFile("data/county_names.csv")

    //2. Split each line into two parts based on comma(“,") character and create pair RDD of each
    val popRDD = popData.map(s => s.split(",")).map(s => (s(0), s(1)))
    val countyRDD = countyNames.map(s => s.split(",")).map(s => (s(0), s(1)))

    //3. Join the two pair RDDs
    val countyPop = countyRDD.join(popRDD)

    //4. Define a class to give the new RDD a structure and create a final flattened RDD
    case class Census(code: Int, county: String, population: BigInt)
    val censusRDD = countyPop.map{case(x, y) => Census(x.toInt, y._1, y._2.toInt)}
    censusRDD.cache()

    //5. Now lets calculate the max, min and avg populations
    val populations = censusRDD.map(r => r.population)
    val min = populations.min
    val max = populations.max
    val avg = populations.reduce(_ + _).toFloat/populations.count
    println("Min: %d, Max: %d, Avg: %10.2f".format(min, max, avg))

    //6. Show the top 10 counties by population
    val topCounties = censusRDD.map(r => (r.population, r.county)).top(10)
    topCounties.foreach(println)

    sc.stop()

  }
}
