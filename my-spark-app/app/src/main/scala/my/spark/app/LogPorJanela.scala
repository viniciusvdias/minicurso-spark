package my.spark.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object LogPorJanela {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Log por janela")
    val ssc = new StreamingContext(conf, Seconds (1))
    val inputDir = args(0)

    val logData = ssc.textFileStream (inputDir)
       .flatMap(line => {
          if (line.contains("INFO")) List(("info", 1))
          else if (line.contains("WARN")) List(("warn", 1))
          else if (line.contains("ERROR")) List(("error", 1))
          else List.empty
       })

    // ou seja, agora estamos agregando lotes de tamanho de janela 6
    // as informações exibidas na saída serão referentes aos dados
    // dos últimos 5 segundos de processamento
    val counts = logData.reduceByKeyAndWindow((a:Int,b:Int) => a + b,
       Seconds(5), Seconds(1))
    counts.print

    ssc.start
    ssc.awaitTermination
  }
}
