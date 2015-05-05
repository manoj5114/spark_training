/**
 * Created by snudurupati on 5/1/15.
 * A brief simple analysis of an offline batch of twitter feed using SparkSQL and UDFs.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object TwitterStudy {

  case class Tweet(tweet_id: String, retweet: String, timestmp: String, source: String, text: String)

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("TwitterStudy").setMaster("local").set("spark.driver.host", "localhost")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD

    //sc.textFile("data/tweets.csv").map(s => s.split(",")).map(s => Tweet(s(0), s(3), s(5), s(6), s(7))).take(5).foreach(println)
    val tweets = sc.textFile("data/tweets.csv").map(s => s.split(",")).map(s => Tweet(s(0), s(3), s(5), s(6), s(7)))
    tweets.registerTempTable("tweets")

    //show the top 10 tweets by the number of re-tweets
    val top10Tweets = sqlContext.sql("""select text, sum(IF(retweet is null, 0, 1)) rtcount from tweets group by text order by rtcount desc limit 10""")
    println("\nThe top 10 retweeted tweets are:\n")
    top10Tweets.collect.foreach(println)

    //define a UDF to extract usernames from the tweets
    def parseTweet(text: String): String = {
      //define a regex pattern to match "@username"
      val PATTERN = """(?<=^|(?<=[^a-zA-Z0-9-_\.]))@([A-Za-z]+[A-Za-z0-9]+)""".r
      val res = PATTERN.findFirstMatchIn(text)
      if (res.isEmpty) {
        //throw new RuntimeException("Cannot parse tweet:" + text)
        "NA"
      }
      else {
        //return the username
        val m = res.get
        m.group(1)
      }
    }

    //register the UDF
    sqlContext.registerFunction("parseTweet", parseTweet _)

    val top10Users = sqlContext.sql("select parseTweet(text) users, sum(1) usercnt from tweets where parseTweet(text) <> 'NA' group by parseTweet(text) order by usercnt desc limit 10")
    println("\nThe top 10 users referenced in the tweets are:\n")
    top10Users.collect.foreach(println)

    sc.stop()
  }

}