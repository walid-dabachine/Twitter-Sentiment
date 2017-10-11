package tweetmining

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object BatchAnalysis {
  val conf = new SparkConf().setMaster("local[6]").setAppName("Spark batch processing")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  def main(args: Array[String]): Unit = {
    
    //Donald Trump
    println("Twitter data batch processing")
    val tweets: RDD[String] = sc.textFile("1Mtweets_en.txt")
    val nb_tweets_donald = tweets.flatMap(text => text.split("\\s")).filter(text => text.contains("@DonaldTrump")).count()
    println(nb_tweets_donald)
    
    //Tweet --> sentiment 
    val sentiment: RDD[(String, Double)] = tweets.map( (tweet) => (tweet, TweetUtilities.getSentiment(tweet)))
    //sentiment.take(5).foreach(println)
    val hashtags: RDD[(String, Double)] = sentiment.flatMap(e => (TweetUtilities.getHashTags(e._1), e._2))
   // val sentiment_hashtag = tweets.foreach( (tweet) => ( my_map = TweetUtilities.getHashTags(tweet).map(hashtag => TweetUtilities.getSentiment(tweet))))
    
    //val sentiment_hashtag = tweets.map( tweet => (TweetUtilities.getHashTags(tweet).foreach( e => println (e, TweetUtilities.getSentiment(tweet))) ))
    //sentiment_hashtag.take(20).foreach(println)
  } 
}