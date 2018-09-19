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

    // Normalize UDF
    val normalize: ((Double, Double) => Double) = (arg0: Double, sum: Double) => { arg0 / sum }
    val normalizeUDF = udf(normalize)

    // GraphFrames
    val graphName = "Twitter_Graph"
    val dse_graphframe = spark.dseGraph(graphName)
    val g = dse_graphframe.gf
    val results = g.pageRank.resetProbability(0.15).tol(0.0001).run()

    // Persist Data to Cassandra
    val pg_sum = results.vertices.select(sum("pagerank") as "pg_sum").collect()(0)(0).toString.toDouble
    val norm_results = results.vertices.select(col("code"), normalizeUDF(col("pagerank"),lit(pg_sum)) as "norm_pg").sort(desc("norm_pg"))
    norm_results.write.cassandraFormat("twitter_results", "pagerank").save()

  }
}
