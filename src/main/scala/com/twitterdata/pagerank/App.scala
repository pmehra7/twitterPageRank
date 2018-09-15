package com.twitterdata.pagerank


import com.datastax.bdp.graph.spark.graphframe._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types._


object App {
  def main(args: Array[String]):Unit = {

    val spark = SparkSession
      .builder
      .appName("Twitter Page Rank GraphFrames")
      .enableHiveSupport()
      .getOrCreate()


    def time[R](block: => R): R = {
        val t0 = System.nanoTime()
        val result = block    // call-by-name
        val t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0) + "ns")
        result
    }

    // GraphFrames
    val graphName = "Twitter_Graph"
    val dse_graphframe = spark.dseGraph(graphName)
    val g = dse_graphframe.gf
    val results = g.pageRank.resetProbability(0.15).tol(0.0001).run()
    println(results.vertices.select("id", "pagerank").show())

  }
}
