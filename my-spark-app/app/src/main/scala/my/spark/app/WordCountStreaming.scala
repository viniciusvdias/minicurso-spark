package my.spark.app

/* WordCount.scala */
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WordCountStreaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word Count Streaming")
    val ssc = new StreamingContext(conf, Seconds (1))

    val dStreamLines = ssc.socketTextStream("localhost", 5432)
    val words = dStreamLines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word,1))
    val counts = pairs.reduceByKey((a,b) => a + b)

    counts.print()

    ssc.start
    ssc.awaitTermination
  }
}