package my.spark.app

// arquivo com nome SparkSQLApp.scala
import org.apache.spark.sql.SparkSession

object SparkSQLApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val numsDf = spark.read.json("file:///vagrant/data/movimentacoes.json")
    println("Esquema do DataFrame:\n")
    numsDf.printSchema()

    spark.stop()
  }
}
