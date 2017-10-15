package getUniversity

/**
 * filt top 100 universities
 *
 */

import org.apache.spark.sql.SparkSession
object getUniversity {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder
        .appName("getUniversity")
        .getOrCreate()
    val input = spark.read.textFile("/output").rdd
    val university = input.filter(line => line.contains("University")).filter(line => !line.contains("Category"))
    val rank = university.map( line => {
      val fields = line.replace("(", "").replace(")", "").split(",")

      var name = fields(0)
      val value = fields(fields.length - 1).toFloat
      if (fields.length > 2) {
        for (i <- 1 to fields.length - 2)
          name = name + ", " + fields(i)
      }
      (name, value)
    }).sortBy(_._2)
    rank.coalesce(1).saveAsTextFile("/University")
    spark.stop()


  }
}
