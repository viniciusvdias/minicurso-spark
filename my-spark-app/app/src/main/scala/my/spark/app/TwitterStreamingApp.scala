package my.spark.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.FilterQuery

object TwitterStreamingApp {
   def main(args: Array[String]) {
      if (args.length < 4) {
         System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
            "<access token> <access token secret>")
         System.exit(1)
      }

      val consumerKey = args(0)
      val consumerSecret = args(1)
      val accessToken = args(2)
      val accessTokenSecret = args(3)

      // Set the system properties so that Twitter4j library used by twitter stream
      // can use them to generate OAuth credentials
      System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
      System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
      System.setProperty("twitter4j.oauth.accessToken", accessToken)
      System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

      val sparkConf = new SparkConf().setAppName("TwitterStreamingApp")

      // filtro de localizacao
      val brazilBox = Array(-73.98,-33.87,-28.63,5.27)
      val usaBox = Array(144.4,-14.8,-64.4,71.6)
      val bhBox = Array(-44.063292,-20.059465,-43.85722,-19.776544)
      val sjBox = Array(-44.574829,-21.435375,-43.98824,-21.040241)
      val nycBox = Array(-74.0, 40.0, -73.0, 41.0)
      val box = brazilBox
      val boundingBox = Array(box.take(2), box.drop(2))
      val locationQuery = new FilterQuery().locations(boundingBox : _*)

      // configuracao do streaming context
      val ssc = new StreamingContext(sparkConf, Seconds(1))
      ssc.sparkContext.setLogLevel("OFF")

      // usuarios que mais tuitaram nos últimos 5 minutos
      //val stream = TwitterUtils
      //   .createFilteredStream(ssc, None, Some(locationQuery))
      //val tweetCountByUser = stream
      //   .map(status => status.getUser.getName)
      //   .map(user => (user,1))
      //   .reduceByKeyAndWindow(_ + _, Seconds(60 * 5))
      //   .map(_.swap)
      //   .transform(_.sortByKey(false))
      //tweetCountByUser.print(3)

      // hash tags mais populares de tweets de contas verificadas
      val stream = TwitterUtils.createFilteredStream(ssc, None, Some(locationQuery))
      val hashTagsMaisPopulares = stream
         .filter(status => status.getUser.isVerified)
         .flatMap(status =>
            status.getText.split(" ").filter(_.startsWith("#")))
         .map(hashtag => (hashtag,1))
         .reduceByKeyAndWindow(_ + _, Seconds(60 * 60))
         .map(_.swap)
         .transform(_.sortByKey(false))
      hashTagsMaisPopulares.print(3)

      ssc.start()
      ssc.awaitTermination()
   }
}
// scalastyle:on println

