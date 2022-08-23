package my.spark.app
/* WordCount.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]): Unit = {

    val inputFile = args(0)
    val conf = new SparkConf().
      setAppName("Word Count")
    
    // criando o SparkContext
    val sc = new SparkContext(conf)
    /* implemente a solução e 
     * imprima o resultado no final (println)
     */
    sc.stop()
  }
}