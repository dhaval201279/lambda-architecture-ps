package batch

import java.lang.management.ManagementFactory

import config.Settings
import domain.Activity
import org.apache.spark.sql.{SQLContext, SaveMode}
import utils.ScalaUtils._
//import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object BatchJob {
  def main(args: Array[String]): Unit = {

    // Setup Spark Context
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)
    val wlc = Settings.WebLogGen

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    /** Initialize input RDD */
    // commented for hdfs based storage. Uncomment for local storage-- val sourceFile = "file:///H:/Dhaval/Tech/pluralsight-lambda-arch/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/data.tsv"
    /*val sourceFile = "file:///vagrant/data.tsv"
    val input = sc.textFile(sourceFile)*/

    /** Spark action results in job execution . . .this is just to check whether above steps are working as per expected behavior*/
    /*input
      .foreach(println)*/

    /** commented below code once we start executing batch by reading data from HDFS*/
    /*val inputDF = input.flatMap{ line =>
      val record = line.split("\\t")
      val MS_IN_HOUR = 1000 * 60 * 60
      if (record.length == 7)
        Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
      else
        None
    }.toDF()*/

    val inputDF = sqlContext.read.parquet(wlc.hdfsPath)
      .where("unix_timestamp() - timestamp_hour / 1000 <= 60 * 60 * 6")

    /*val keyedByProduct = inputDF.keyBy(a => (a.product, a.timestamp_hour)).cache()
    val visitorByProduct = keyedByProduct
                            .mapValues(a => a.visitor)
                            .distinct()
                            .countByKey()

    val activityByProduct = keyedByProduct
      .mapValues{ a =>
        a.action match {
          case "purchase" => (1,0,0)
          case "add_to_cart" => (0,1,0)
          case "page_view" => (0,0,1)
        }

      }
      .reduceByKey( (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))

    visitorByProduct
      .foreach(println)

    activityByProduct
      .foreach(println)*/

    /** since we already have data based on hdfs based impl comment below code*/
    /*val df = inputDF.select(
      add_months(from_unixtime(inputDF("timestamp_hour") / 1000), 1).as("timestamp_hour"),
      inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"), inputDF("product")
    ).cache()*/

    //df.registerTempTable("activity")
    inputDF.registerTempTable("activity")

    val visitorsByProduct = sqlContext.sql(
      """SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
        |FROM activity GROUP BY product, timestamp_hour
      """.stripMargin)

    // for cassandra
    visitorsByProduct
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "lambda", "table" -> "batch_visitors_by_product"))
      .save()

    /**  Since we dont need to print schema we are commenting it
      *
    visitorsByProduct.printSchema()
      */


      // we will cache it otherwise this will be executed twice, one for 'write' and other for 'foreach' below
    val activityByProduct = sqlContext.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """).cache()

    activityByProduct
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "lambda", "table" -> "batch_activity_by_product"))
      .save()

    /**  Since we dont need we are commenting it
      *
    activityByProduct.registerTempTable("activityByProduct")
    //activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/lambda/batch1")
    */

    /**  Since we dont need underExposedProduct we are commenting it
      *
    val underExposedProducts = sqlContext.sql("""SELECT
        product,
        timestamp_hour,
         UnderExposed(page_view_count, purchase_count) as negative_exposure
        from activityByProducts
         order by negative_exposure DESC
         limit 5
        """)*/


    visitorsByProduct.foreach(println)
    activityByProduct.foreach(println)
  }

}
