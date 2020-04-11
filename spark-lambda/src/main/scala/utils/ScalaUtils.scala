package utils

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object ScalaUtils {
  def getSparkContext(appName: String) = {
    // Get Spark Configurations
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.cassandra.connection.host", "localhost")

    var checkPointDirectory = ""

    if (isIDE) {
      System.setProperty("hadoop.home.dir", "H:\\Dhaval\\Tech\\pluralsight-lambda-arch\\exercise files") // required for winutils
      conf.setMaster("local[*]")
      checkPointDirectory = "file:///H:/Dhaval/Tech/pluralsight-lambda-arch/temp"
    } else {
      checkPointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
    }

    /** setup spark context */
    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkPointDirectory)
    sc

  }

  def getSQLContext(sc: SparkContext): SQLContext = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }

  def getStreamingContext(streamingApp: (SparkContext, Duration) => StreamingContext, sc: SparkContext, batchDuration: Duration )= {
    //val creatingFunc : () => StreamingContext = () => streamingApp(sc, batchDuration)
    // simplified way of writing above construct
    val creatingFunc = () => streamingApp(sc, batchDuration)

    val ssc = sc.getCheckpointDir match {
      case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach(cp => ssc.checkpoint(cp))
    ssc
  }

  /** Check if running from IDE */
  val isIDE = {
    ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
  }

}
