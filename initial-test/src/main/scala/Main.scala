//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import org.apache.spark.sql.functions._

object Main extends App {
    println("Hello, World!")

    val spark = SparkSession.builder.master("local").getOrCreate()
    import spark.implicits._
    //spark.sparkContext.setLogLevel("INFO")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1")

    val df = spark.sparkContext.makeRDD(1 to 100000000, 5).toDF
    val df2 = spark.sparkContext.makeRDD(1 to 100000000, 5).toDF
    //println("Size: "+SizeEstimator.estimate(df)+" bytes")
    
    val count = df.select($"value" as "a").filter("a > 10000000").join(broadcast(df2.select($"value" as "b").filter("b < 70000000")), $"a" === $"b").count

    println(count)

    // for maintaining web UI
    //Thread.sleep(86400000);
    //spark.sparkContext.stop();
}
