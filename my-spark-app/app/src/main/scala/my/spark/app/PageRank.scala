package my.spark.app

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object PageRank {
  def main (args: Array[String]): Unit = {
    // sparkConf
    val conf = new SparkConf().setAppName("Road to page rank")

    // criando o SparkContext, ponto de partida para o paralelismo
    val sc = new SparkContext(conf)

    // linhas do arquivo de arestas
    val gLinesRDD = sc.textFile(args(0))

    // iteracoes do algoritmo
    val iterations = args(1).toInt

    // arestas
    val edgesRDD = gLinesRDD
       .filter (line => !line.isEmpty && line(0) != '#')
       .map(line => {
         val vertices = line.split (" |\t")
         (vertices(0).toInt, vertices(1).toInt)
       })

    // vertices
    val verticesRDD = edgesRDD
       .flatMap(tup => List(tup._1, tup._2))
       .distinct()

    // listas de ajacências dos vértices
    val adjListsRDD = edgesRDD.groupByKey()
       .partitionBy(new HashPartitioner(16*4))
       .cache()

    // rank de cada vértice começa com 1.0
    var ranksRDD = verticesRDD.map(u => (u,1.0))

    // iteracoes do pagerank
    for (i <- 1 to iterations) {
      ranksRDD = adjListsRDD.join(ranksRDD)
         .flatMap(urlLinksRank => {
           val (url, linksRank) = urlLinksRank
           val (links, rank) = linksRank
           links.map(dest => (dest, rank/links.size))
         })
         .reduceByKey(_ + _)
    }

    // ordena vertices e respectivos ranqueamentos
    val sortedByRankRDD = ranksRDD.sortBy(vertexRank => -vertexRank._2)
    println("ranking sample (4) = " + sortedByRankRDD.take(4).mkString(","))

    sc.stop()
  }
}
