import java.util
import java.sql.Timestamp
import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object FileStream {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder
      .master("local[*]")
      //.appName("Thesis")
      .config("spark.sql.streaming.metricsEnabled", true)
      .config("spark.eventLog.enabled",true)
      .config("spark.logConf", true)
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    var avg_processed_rows_per_sec: Double = 0.0
    var batch_num = 0

    val schema_web_page = StructType(
      Array(
          StructField("wp_web_page_sk", IntegerType),
          StructField("wp_web_page_id", StringType),
          StructField("wp_rec_start_date", DateType),
          StructField("wp_rec_end_date", DateType),
          StructField("wp_creation_date_sk", IntegerType),
          StructField("wp_access_date_sk", IntegerType),
          StructField("wp_autogen_flag", StringType),
          StructField("wp_customer_sk", IntegerType),
          StructField("wp_url", StringType),
          StructField("wp_type", StringType),
          StructField("wp_char_count", IntegerType),
          StructField("wp_link_count", IntegerType),
          StructField("wp_image_count", IntegerType),
          StructField("wp_max_ad_count", IntegerType)))
    
    val schema_web_site = StructType(Array(
          StructField("web_site_sk", IntegerType),
          StructField("web_site_id", StringType),
          StructField("web_rec_start_date", DateType),
          StructField("web_rec_end_date", DateType),
          StructField("web_name", StringType),
          StructField("web_open_date_sk", IntegerType),
          StructField("web_close_date_sk", IntegerType),
          StructField("web_class", StringType),
          StructField("web_manager", StringType),
          StructField("web_mkt_id", IntegerType),
          StructField("web_mkt_class", StringType),
          StructField("web_mkt_desc", StringType),
          StructField("web_market_manager", StringType),
          StructField("web_company_id", IntegerType),
          StructField("web_company_name", StringType),
          StructField("web_street_number", StringType),
          StructField("web_street_name", StringType),
          StructField("web_street_type", StringType),
          StructField("web_suite_number", StringType),
          StructField("web_city", StringType),
          StructField("web_county", StringType),
          StructField("web_state", StringType),
          StructField("web_zip", StringType),
          StructField("web_country", StringType),
          StructField("web_gmt_offset", DoubleType), // should be DecimalType(5, 2)
          StructField("web_tax_percentage", DoubleType))) // should be DecimalType(5, 2)
    
    /*val schema_web_sales = StructType(
      Array(StructField("ws_transaction_id", IntegerType),
        StructField("ws_user_id", IntegerType),
        StructField("ws_product_id", IntegerType),
        StructField("ws_quantity", FloatType),
        StructField("ws_timestamp", TimestampType)))

    val schema_web_logs = StructType(
      Array(StructField("wl_customer_id", LongType ,nullable = true),
        StructField("wl_id", LongType ,nullable = true),
        StructField("wl_item_id", LongType ,nullable = true),
        StructField("wl_key1", LongType ,nullable = true),
        StructField("wl_key10", LongType ,nullable = true),
        StructField("wl_key100", LongType ,nullable = true),
        StructField("wl_key11", LongType ,nullable = true),
        StructField("wl_key12", LongType ,nullable = true),
        StructField("wl_key13", LongType ,nullable = true),
        StructField("wl_key14", LongType ,nullable = true),
        StructField("wl_key15", LongType ,nullable = true),
        StructField("wl_key16", LongType ,nullable = true),
        StructField("wl_key17", LongType ,nullable = true),
        StructField("wl_key18", LongType ,nullable = true),
        StructField("wl_key19", LongType ,nullable = true),
        StructField("wl_key2", LongType ,nullable = true),
        StructField("wl_key20", LongType ,nullable = true),
        StructField("wl_key21", LongType ,nullable = true),
        StructField("wl_key22", LongType ,nullable = true),
        StructField("wl_key23", LongType ,nullable = true),
        StructField("wl_key24", LongType ,nullable = true),
        StructField("wl_key25", LongType ,nullable = true),
        StructField("wl_key26", LongType ,nullable = true),
        StructField("wl_key27", LongType ,nullable = true),
        StructField("wl_key28", LongType ,nullable = true),
        StructField("wl_key29", LongType ,nullable = true),
        StructField("wl_key3", LongType ,nullable = true),
        StructField("wl_key30", LongType ,nullable = true),
        StructField("wl_key31", LongType ,nullable = true),
        StructField("wl_key32", LongType ,nullable = true),
        StructField("wl_key33", LongType ,nullable = true),
        StructField("wl_key34", LongType ,nullable = true),
        StructField("wl_key35", LongType ,nullable = true),
        StructField("wl_key36", LongType ,nullable = true),
        StructField("wl_key37", LongType ,nullable = true),
        StructField("wl_key38", LongType ,nullable = true),
        StructField("wl_key39", LongType ,nullable = true),
        StructField("wl_key4", LongType ,nullable = true),
        StructField("wl_key40", LongType ,nullable = true),
        StructField("wl_key41", LongType ,nullable = true),
        StructField("wl_key42", LongType ,nullable = true),
        StructField("wl_key43", LongType ,nullable = true),
        StructField("wl_key44", LongType ,nullable = true),
        StructField("wl_key45", LongType ,nullable = true),
        StructField("wl_key46", LongType ,nullable = true),
        StructField("wl_key47", LongType ,nullable = true),
        StructField("wl_key48", LongType ,nullable = true),
        StructField("wl_key49", LongType ,nullable = true),
        StructField("wl_key5", LongType ,nullable = true),
        StructField("wl_key50", LongType ,nullable = true),
        StructField("wl_key51", LongType ,nullable = true),
        StructField("wl_key52", LongType ,nullable = true),
        StructField("wl_key53", LongType ,nullable = true),
        StructField("wl_key54", LongType ,nullable = true),
        StructField("wl_key55", LongType ,nullable = true),
        StructField("wl_key56", LongType ,nullable = true),
        StructField("wl_key57", LongType ,nullable = true),
        StructField("wl_key58", LongType ,nullable = true),
        StructField("wl_key59", LongType ,nullable = true),
        StructField("wl_key6", LongType ,nullable = true),
        StructField("wl_key60", LongType ,nullable = true),
        StructField("wl_key61", LongType ,nullable = true),
        StructField("wl_key62", LongType ,nullable = true),
        StructField("wl_key63", LongType ,nullable = true),
        StructField("wl_key64", LongType ,nullable = true),
        StructField("wl_key65", LongType ,nullable = true),
        StructField("wl_key66", LongType ,nullable = true),
        StructField("wl_key67", LongType ,nullable = true),
        StructField("wl_key68", LongType ,nullable = true),
        StructField("wl_key69", LongType ,nullable = true),
        StructField("wl_key7", LongType ,nullable = true),
        StructField("wl_key70", LongType ,nullable = true),
        StructField("wl_key71", LongType ,nullable = true),
        StructField("wl_key72", LongType ,nullable = true),
        StructField("wl_key73", LongType ,nullable = true),
        StructField("wl_key74", LongType ,nullable = true),
        StructField("wl_key75", LongType ,nullable = true),
        StructField("wl_key76", LongType ,nullable = true),
        StructField("wl_key77", LongType ,nullable = true),
        StructField("wl_key78", LongType ,nullable = true),
        StructField("wl_key79", LongType ,nullable = true),
        StructField("wl_key8", LongType ,nullable = true),
        StructField("wl_key80", LongType ,nullable = true),
        StructField("wl_key81", LongType ,nullable = true),
        StructField("wl_key82", LongType ,nullable = true),
        StructField("wl_key83", LongType ,nullable = true),
        StructField("wl_key84", LongType ,nullable = true),
        StructField("wl_key85", LongType ,nullable = true),
        StructField("wl_key86", LongType ,nullable = true),
        StructField("wl_key87", LongType ,nullable = true),
        StructField("wl_key88", LongType ,nullable = true),
        StructField("wl_key89", LongType ,nullable = true),
        StructField("wl_key9", LongType ,nullable = true),
        StructField("wl_key90", LongType ,nullable = true),
        StructField("wl_key91", LongType ,nullable = true),
        StructField("wl_key92", LongType ,nullable = true),
        StructField("wl_key93", LongType ,nullable = true),
        StructField("wl_key94", LongType ,nullable = true),
        StructField("wl_key95", LongType ,nullable = true),
        StructField("wl_key96", LongType ,nullable = true),
        StructField("wl_key97", LongType ,nullable = true),
        StructField("wl_key98", LongType ,nullable = true),
        StructField("wl_key99", LongType, nullable = true),
        StructField("wl_timestamp", StringType, nullable = true),
        StructField("wl_webpage_name", StringType, nullable = true)))*/
    
    /**===================================================================
      * Set paths and read options for streams*/

    /*val pathweb_logs = ("C:\\Sparkfiles\\web_logs")
    val web_logsDF = sparkSession.readStream
      .option("header", "false")
      .option("maxFilesPerTrigger",1)
      .schema(schema_web_logs)
      .json(pathweb_logs)

    val pathweb_sales = ("C:\\Sparkfiles\\web_sales")
    val web_salesDF = sparkSession.readStream
      .option("header", "false")
      .option("maxFilesPerTrigger",1)
      .schema(schema_web_sales)
      .text(pathweb_sales)*/

    val path_tpcds_csv = "/home/sue/spark-gpu-tests/structured-streaming-test/spark-rapids/integration_tests/src/test/resources/tpcds/csv/"
    val path_tpcds_parquet = "/home/sue/spark-gpu-tests/structured-streaming-test/spark-rapids/integration_tests/src/test/resources/tpcds/parquet/"
    val sf_1 = "sf_1/"
    val sf_10 = "sf_10/"

    val web_pageDF = sparkSession.readStream
      .option("maxFilesPerTrigger", 1)
      .schema(schema_web_page)
      .csv(path_tpcds_csv + sf_1 + "web_page.dat")
    
    val web_siteDF = sparkSession.readStream
      .option("maxFilesPerTrigger", 1)
      .schema(schema_web_site)
      .csv(path_tpcds_csv + sf_1 + "web_site.dat")

    /**val web_salesStaticDf = sparkSession.read
      .option("header", "false")
      .schema(schema_web_sales)
      .text(pathweb_sales)*/

    //println(web_salesDF.isStreaming)
    //println(web_logsDF.isStreaming)
    println(web_pageDF.isStreaming)
    println(web_siteDF.isStreaming)

    //web_logsDF.printSchema()
    //web_logsDF.createOrReplaceTempView("web_logs")

    //web_salesDF.printSchema()
    //web_salesDF.createOrReplaceTempView("web_sales")

    web_pageDF.printSchema()
    //web_pageDF.createOrReplaceTempView("web_page")

    web_siteDF.printSchema()
    //web_siteDF.createOrReplaceTempView("web_site")

    /**web_salesStaticDf.printSchema()
    web_salesStaticDf.createOrReplaceTempView("web_sales_static")*/


    /**=====================================================================
      * Set options for writestream
      */

    //val withEventTime = web_pageDF.selectExpr("*", "cast(cast(wp_creation_date_sk as double)/1000000000 as timestamp) as creation_time")
    val web_pageDF_withEventTime = web_pageDF.selectExpr("*").withColumn("timestamp", current_timestamp())
    val web_siteDF_withEventTime = web_siteDF.selectExpr("*").withColumn("timestamp", current_timestamp())
    // TPCDS data q1
    /*var web_page_q = web_pageDF
      .groupBy("wp_type").count()
      .orderBy("count")
      .select("wp_type", "count")
      .where("wp_type IS NOT NULL")*/

    /**========== Query 22 =========== */
    /*var web_logs_22 = web_logsDF
      .groupBy("wl_customer_id").count()
      .orderBy("count")
      .select("wl_customer_id","count")
      .where("wl_customer_id IS NOT NULL")

    /** ========= Query Milk ========= */
    var web_sales_milk = web_salesDF
      .groupBy("ws_product_id").count()
      .orderBy("ws_product_id")
      .select("ws_product_id","count")
      .where("ws_product_ID IS NOT NULL")

    /**========= Query 16 ========== */
    var web_logs_16 = web_logsDF
      .groupBy("wl_webpage_name").count()
      .orderBy("count")
      .select("wl_webpage_name","count")
      .where("wl_webpage_name IS NOT NULL")

    /** ========= Query 05 =========== */
    var web_logs_05 = web_logsDF
      .groupBy("wl_item_id").count()
      .orderBy("count")
      .select("wl_item_id","count").where("wl_item_id IS NOT NULL")

    /** ========= Query 06 ========= */
    var browsedDF = web_logsDF
      .sparkSession.sql("SELECT wl_item_id AS br_id, COUNT(wl_item_id) AS br_count FROM web_logs WHERE wl_item_id IS NOT NULL GROUP BY wl_item_id")
      .createOrReplaceTempView("browsed")

    /**org.apache.log4j.filter.StringMatchFilter=*/


    var purchasedDF = web_salesStaticDf
      .sparkSession.sql("SELECT ws_product_id AS pu_id FROM web_sales_static WHERE ws_product_id IS NOT NULL GROUP BY ws_product_id")
      .createOrReplaceTempView("purchased")


    var query06 = web_logsDF
      .sparkSession.sql("SELECT br_id, COUNT(br_id) FROM browsed LEFT JOIN purchased ON browsed.br_id = purchased.pu_id WHERE purchased.pu_id IS NULL GROUP BY browsed.br_id")*/

    println("Queries start now")
/** ============ Start Streamqueries ========== */

    //testing TPCDS query
    var q1 = web_siteDF_withEventTime.select("web_mkt_id", "web_mkt_class", "web_mkt_desc", "web_open_date_sk", "web_close_date_sk").where("web_close_date_sk IS NOT NULL").groupBy(col("web_mkt_desc")).count()
      .writeStream
      .queryName("TPCDS-q1")
      .format("console")
      .outputMode("complete")
      .start()//.awaitTermination()

    var q2 = web_pageDF_withEventTime.groupBy(col("wp_type")).count()
      .writeStream
      .queryName("TPCDS-q2")
      .format("console")
      .outputMode("complete") //executing w/o timestamp is available only when output mode is 'complete'
      .start()//.awaitTermination()

    /*var query16 = web_logs_16.writeStream
      .format("console")
      .queryName("+++16+++")
      /**.trigger(Trigger.ProcessingTime("150 seconds"))*/
      .outputMode(OutputMode.Complete())
      //.start()


    var query22 = web_logs_22.writeStream
      .format("console")
      .queryName("22")
      .outputMode(OutputMode.Complete())


    var query05 = web_logs_05.writeStream
      .format("console")
      .queryName("+++05+++")
      /**.trigger(Trigger.ProcessingTime("150 seconds"))*/
      .outputMode(OutputMode.Complete())
      /**.start()*/

    /**var query06join = query06.writeStream
      .format("console")
      .outputMode(OutputMode.Complete())*/

    var querymilk = web_sales_milk.writeStream
      .format("console")
      .queryName("+++milk+++")
      /**.trigger(Trigger.ProcessingTime("80 seconds"))*/
      .outputMode(OutputMode.Complete())
      /**.start()*/*/

    sparkSession.streams.addListener(new StreamingQueryListener(){
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println("******************" + "Query started: " + event.id + "******************************************")
      }
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        avg_processed_rows_per_sec = avg_processed_rows_per_sec.asInstanceOf[Double] + event.progress.processedRowsPerSecond.asInstanceOf[Double]
        batch_num = batch_num + 1

        println("************************************************************")
        //println("Query Information:  " + event.progress.name + " Metric: " + event.progress.prettyJson)
        println("Processed Rows Per Second: " + event.progress.name + " Metric: " + event.progress.processedRowsPerSecond)
        println("Number of Input Rows: " + event.progress.name + " Metric: " + event.progress.numInputRows)
        println("Input Rows Per Second: " + event.progress.name + " Metric: " + event.progress.inputRowsPerSecond)
        println("Average Processed Rows Per Second: " + avg_processed_rows_per_sec / batch_num + "!!")
        println("************************************************************")
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println("******************" + "Query terminated: " + event.id + "******************************************")
      }
    })

    q1.awaitTermination()
    q2.awaitTermination()

     /**query05.awaitTermination()*/
     /**query22.awaitTermination()*/
     /**query16.awaitTermination()*/
     /**querymilk.awaitTermination()*/
     /**query06join.awaitTermination()*/

    // for maintaining web UI
    Thread.sleep(86400000);
    sparkSession.sparkContext.stop();

  }

}
