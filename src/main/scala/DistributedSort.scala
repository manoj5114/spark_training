/**
 * Created by snudurupati on 5/1/15.
 * Sort a list of strings alphabetically based on first character
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object DistributedSort {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("DistributedSort").setMaster("local").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)

    //Create an RDD from a list of strings
    val bigCats = List("cheetah", "leopard", "cougar", "jaguar","lion", "snow leopard", "tiger")
    val listRDD = sc.parallelize(bigCats)

    //Transform the above RDD into a pair RDD such that key is the first character and value is the name. e.g. (‘l’, “lion”)
    val pairRDD = listRDD.keyBy(s => s(0))

    //Sort the pair RDD in ascending order
    val sortedRDD = pairRDD.sortByKey()

    //Print just the values of the sorted RDD
    sortedRDD.collect.foreach{case(k, v) => println(v)}

  }

}