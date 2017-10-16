package GraphXPRMain

/**
 * PageRank use graphX
 *
 */
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SparkSession
object PageRankExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"GraphXPR")
      .getOrCreate()
    val sc = spark.sparkContext

    val graph1 = GraphLoader.edgeListFile(sc, "/edge").cache()

    val ranks = graph1.pageRank(0.0001).vertices

    val titles = sc.textFile("/id/part-00000").map { line =>
      val fields = line.replace("(", "").replace(")", "").split(",")
      var name = fields(0)
      if (fields.length > 2) {
        for (i <- 1 to fields.length - 2)
          name = name + ", " + fields(i)
      }
      (fields.last.toLong, name)
    }
    val result = titles.join(ranks).map {
      case (id, (title, rank)) => (title, rank)
    }.sortBy(_._2, false)





    result.coalesce(1).saveAsTextFile("/GraphXoutput")


    // $example off$
    spark.stop()
  }
}
