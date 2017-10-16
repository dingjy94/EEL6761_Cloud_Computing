package GraphxPageRank

/**
 * Pre processing of graphX pagerank
 * get two file
 * id:  each line is (title, VertexId)
 * edge: each line is {VertexId1, VertexId2} 
 *
 */
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd._
import java.security.MessageDigest

import org.apache.spark.SparkContext

import scala.xml.{NodeSeq, XML}
import org.apache.spark.graphx.Graph._
import org.apache.spark.graphx.VertexId


object GraphPageRank {

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }

    showWarning()

    val spark = SparkSession
      .builder
      .appName("PageRank")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val iters = if (args.length > 1) args(1).toInt else 10
    val lines = spark.read.textFile(args(0)).rdd
    val edges= lines.map ( line => {

      val fields = line.split("\t")
      val (title, body) = (fields(1), fields(3).replace("\\n", "\n"))
      val targets =
        if (body == "\\N") {
          NodeSeq.Empty
        } else {
          try {
            XML.loadString(body) \\ "link" \ "target"
          } catch {
            case e: org.xml.sax.SAXParseException =>
              System.err.println("Article \"" + title + "\" has malformed XML in body:\n" + body)
              NodeSeq.Empty
          }
        }
      val targetsText = targets.map(target => new String(target.text.toLowerCase())).toArray
      (new String(title).toLowerCase(), targetsText)

    }).flatMap(pair =>
      pair._2.map(arrayElem => (pair._1, arrayElem))
    ).cache()

    var index:VertexId = 0L
    val id = lines.map( line =>  {
      val title = line.split("\t")(1)
      index = index + 1L
      (title.toLowerCase(), index)
    }).cache()

    val indexTitle = sc.broadcast(id.collectAsMap())

    val relation = edges.map(edge => {
      if (indexTitle.value.contains(edge._1))
        if (indexTitle.value.contains(edge._2))
          indexTitle.value(edge._1) + " " + indexTitle.value(edge._2)
    })

    relation.saveAsTextFile("/edge")
    id.saveAsTextFile("/id")
    spark.stop()
  }
}
