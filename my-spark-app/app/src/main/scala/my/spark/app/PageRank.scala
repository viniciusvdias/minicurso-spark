package my.spark.app
/* PageRank.scala */
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

// configurar logging por aplicação
import org.apache.log4j.Logger
import org.apache.log4j.Level

object PageRank {
  def main (args: Array[String]): Unit = {
    val log = Logger.getLogger("Page rank")
    log.setLevel(Level.DEBUG)

    // sparkConf
    val conf = new SparkConf().setAppName("Road to page rank")
    // criando o SparkContext, ponto de partida para o paralelismo
    val sc = new SparkContext(conf)

    val gLinesRDD = sc.textFile (args(0))
    log.debug ("rdd de linhas do arquivo de arestas ..")
     
    // arestas
    val edgesRDD = gLinesRDD.
      filter (line => !line.isEmpty && line(0) != '#').
      map {line =>
        val vertices = line.split (" |\t")
        (vertices(0).toInt, vertices(1).toInt)
      }
    log.debug ("rdd de arestas do grafo ..")
      
    // vertices
    val verticesRDD = edgesRDD.
      flatMap {case (v1,v2) => Iterator (v1,v2)}.
      distinct()
    log.debug ("rdd de vértices do grafo ..")
      
    // listas de ajacências dos vértices
    val adjListsRDD = edgesRDD.groupByKey().
      partitionBy (new HashPartitioner(16*4)).
      cache()
    log.debug ("rdd de listas de adjacências ..")
      
    // rank de cada vértice começa com 1.0
    var ranksRDD = verticesRDD.map ( (_,1.0) )
    //var ranksRDD = edgesRDD.map (_._1).distinct.map (v => (v,1.0))
    log.debug ("rdd de ranks iniciais de cada vértice (ou seja, 1.0)")
     
    // pagerank
    val iterations = args(1).toInt
    for (i <- 1 to iterations) {
      ranksRDD = adjListsRDD.join(ranksRDD).
        flatMap {case (url, (links, rank)) =>
          links.map (dest => (dest, rank/links.size))
        }.
        reduceByKey(_ + _).
        mapValues(0.15 + 0.85 * _)
    }
    log.debug (iterations + " rdds de ranks atualizados foram criados")

    val sortedByRankRDD = ranksRDD.sortBy(-_._2)

    log.debug ("ranking sample (4) = " + sortedByRankRDD.take(4).mkString(","))

    sc.stop()
  }
}
