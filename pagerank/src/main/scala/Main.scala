import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
//import org.apache.spark.util.SizeEstimator

import org.apache.spark.graphx.GraphLoader

object Main extends App {
    
    val spark = SparkSession.builder.master("local").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("INFO")
    val sc = spark.sparkContext
    
    val graph = GraphLoader.edgeListFile(sc, "/home/sue/spark-gpu-tests/pagerank/data/followers.txt")
    
    val ranks = graph.pageRank(0.0001).vertices
    val users = sc.textFile("/home/sue/spark-gpu-tests/pagerank/data/users.txt").map{line =>
        val fields = line.split(",")
        (fields(0).toLong, fields(1))
    }
    
    val ranksByUsername = users.join(ranks).map{
        case(id, (username, rank)) => (username, rank)
    }

    println(ranksByUsername.collect().mkString("\n"))

    // for maintaining web UI
    Thread.sleep(86400000);
    spark.sparkContext.stop();
}
