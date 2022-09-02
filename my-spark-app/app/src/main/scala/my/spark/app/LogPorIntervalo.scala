package my.spark.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogPorIntervalo {
   def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Log por janela")
      val ssc = new StreamingContext (conf, Seconds (5))
      val inputDir = args(0)

      val logData = ssc.textFileStream(inputDir)
         .flatMap(line => {
            if (line.contains("INFO")) List(("info", 1))
            else if (line.contains("WARN")) List(("warn", 1))
            else if (line.contains("ERROR")) List(("error", 1))
            else List.empty
         })

      val counts = logData.reduceByKey(_ + _)
      counts.print(10)

      ssc.start
      ssc.awaitTermination
   }
}
