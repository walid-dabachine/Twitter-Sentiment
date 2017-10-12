package tweetmining

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object BatchAnalysis {
  val conf = new SparkConf().setMaster("local[6]").setAppName("Spark batch processing")
  val sc = new SparkContext(conf)
  val k = 10
  sc.setLogLevel("WARN")
  
  def main(args: Array[String]): Unit = {
    
    //Donald Trump
    println("Twitter data batch processing")
    val tweets: RDD[String] = sc.textFile("1Mtweets_en.txt")
    val nb_tweets_donald = tweets.flatMap(text => text.split("\\s")).filter(text => text.contains("@DonaldTrump")).count()
    println("Nbr de tweets de Donald Trump: "+nb_tweets_donald)
    
    //Tweet --> sentiment 
    val sentiment: RDD[(String, Double)] = tweets.map( (tweet) => (tweet, TweetUtilities.getSentiment(tweet)))
    
    //sentiment par hastag
    //De la forme (Set(String), Double)
    var hashtag = sentiment.flatMap(f => List((TweetUtilities.getHashTags(f._1), f._2))).filter(f => f._1.nonEmpty)
    //hashtag.foreach(println)
    
    //top-K hashtag (pas optimise :( )
    var swap = hashtag.map(_.swap)
    var k_worst = swap.sortBy(_._1).take(k)
    var k_best = swap.sortByKey(false, 0).take(k)
    
    println("----Worst tweets----")
    k_worst.foreach(println)
    println("----Best tweets----")
    k_best.foreach(println)
   
  } 
}