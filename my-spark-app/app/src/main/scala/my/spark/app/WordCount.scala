package my.spark.app
/* WordCount.scala */
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    val inputFile = args(0)
    val conf = new SparkConf().
      setAppName("Word Count")
    // criando o SparkContext
    val sc = new SparkContext(conf)

    val lines = sc.textFile(inputFile)
    val words = lines.flatMap(line => line.split(" "))
    val intermData = words.map(word => (word,1))
    val wordCount = intermData.reduceByKey((a,b) => a + b)
    val contagens = wordCount
       .filter(kv => kv._2 > 1)
       .take(5)

    println("contagens = " + contagens.mkString(","))
    println("count = " + wordCount.count)

    sc.stop()
  }
}