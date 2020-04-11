package streaming

import domain.{Activity, ActivityByProduct, VisitorsByProduct}
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import breeze.linalg.max
import org.apache.spark.SparkContext
import functions._
import org.apache.spark.streaming._
import utils.ScalaUtils._
import com.twitter.algebird.HyperLogLogMonoid
import config.Settings
import javax.xml.soap.SOAPMessage
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import _root_.kafka.common.TopicAndPartition
import _root_.kafka.message.MessageAndMetadata
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._


object StreamingJob {
  def main(args: Array[String]): Unit = {
    // Setup Spark Context
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)

    import sqlContext.implicits._

    val batchDuration = Seconds(4)
    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)

      /**----------------------- Kafka start*/
      val wlc = Settings.WebLogGen
      val topic = wlc.kafkaTopic

      val kafkaParams = Map (
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "lambda",
        "auto.offset.reset" -> "largest"
      )

      // for using direct api i.e. Simple ApI
      val kafkaDirectParams = Map (
        "metadata.broker.list" -> "localhost:9092",
        "group.id" -> "lambda",
        "auto.offset.reset" -> "smallest"
      )

      val hdfsPath = wlc.hdfsPath
      val hdfsData = sqlContext.read.parquet(hdfsPath)

      val fromOffsets = hdfsData
                          .groupBy("topic", "kafkaPartition")
                          .agg(Map( "untilOffset" -> "untilOffset"))
                          .collect().map{ row =>
                            (TopicAndPartition(row.getAs[String]("topic"), row.getAs[Int]("kafkaPartition")),
                              row.getAs[String]("untilOffset").toLong + 1)
                          }.toMap

      val kafkaStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder] (
        ssc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_AND_DISK
      ).map(_._2)

      val kafkaDirectStream = fromOffsets.isEmpty match {
        case true =>
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
                                    ssc, kafkaDirectParams, Set(topic))

        case false =>
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
            ssc, kafkaDirectParams, fromOffsets, { mmd : MessageAndMetadata[String, String] => (mmd.key(), mmd.message()) }
          )
      }
      /** -----------------------------Kafka end*/
        /** commented after kafka impl*/
      /*val inputPath = isIDE match {
        case true => "file:///H:/Dhaval/Tech/pluralsight-lambda-arch/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
        case false => "file:///vagrant/input"
      }

      val textDStream = ssc.textFileStream(inputPath)*/
      /** replaced textDStream with kafkaStream after kafka based impl*/
      //val activityStream = textDStream.transform(input => {
      // commented to use direct API i.e. Simple API val activityStream = kafkaStream.transform(input => {
      val activityStream = kafkaDirectStream.transform(input => {
        functions.rddToRDDActivity(input)
      }).cache()


      /** as part of kafka section - we want to save data to hdfs Start
        * ideally below code should be extracted into a separate
        * */

      activityStream.foreachRDD { rdd =>
        val activityDF = rdd
                          .toDF()
                          .selectExpr("timestamp_hour", "referrer", "action", "prevPage", "page", "visitor",
                            "product", "inputProps.topic as topic", "inputProps.kafkaPartition as kafkaPartition",
                            "inputProps.fromOffset as fromOffset", "inputProps.untilOffset as untilOffset")
        activityDF
          .write
          .partitionBy("topic", "kafkaPartition", "timestamp_hour")
          .mode(SaveMode.Append)
          .parquet(hdfsPath)
      }
      /** as part of kafka section - we want to save data to hdfs End */
      // from below code variable initialization is for using mapWithState
      val activityStateSpec = StateSpec
        .function(mapActivityStateFunc)
        .timeout(Minutes(120))
      // till above code variable initialization is for using mapWithState

      val statefulActivityByProduct = activityStream.transform(rdd => {
        val df = rdd.toDF()
        df.registerTempTable("activity")

        val activityByProduct = sqlContext.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """)

        activityByProduct
          .map{r => ((r.getString(0), r.getLong(1)),
            ActivityByProduct(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))
          )}
      })//.print() -- below code for checkpoint management along with states
        .mapWithState(activityStateSpec)

      val activityStateSnapshot= statefulActivityByProduct.stateSnapshots()
      activityStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4)
        ) // Only saves or expose snapshot every x seconds x in this case is 30
        /******* commented for inserting data to cassandra ----- .foreachRDD(rdd => rdd*/
        .map(sr => ActivityByProduct(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3))
        .saveToCassandra("lambda", "stream_activity_by_product")
        /****
          * commented for inserting data to Cassandra as part of last section
          * .toDF()
        .registerTempTable("ActivityByProduct")*/
      //)
          /**
            * Below code has been commented to demonstrate updateStateByKey which can also be realized by using mapWithStage
            *
            * .updateStateByKey((newItemsPerKey: Seq[ActivityByProduct], currentState: Option[(Long, Long, Long, Long)]) => {
            var (prevTimestamp, purchase_count, add_to_cart_count, page_view_count) = currentState.getOrElse((System.currentTimeMillis(), 0L, 0L, 0L))
            var result : Option[(Long, Long, Long, Long)] = null

            if(newItemsPerKey.isEmpty) {
              if(System.currentTimeMillis() - prevTimestamp > 30000 + 4000)
                result = None
              else
                result = Some(prevTimestamp, purchase_count, add_to_cart_count, page_view_count)
            } else {

              newItemsPerKey.foreach(a => {
                purchase_count += a.purchase_count
                add_to_cart_count += a.add_to_cart_count
                page_view_count += a.page_view_count
              })

              result = Some((System.currentTimeMillis(), purchase_count, add_to_cart_count, page_view_count))
            }
          result
      } )*/

      /** unique visitors by product */
      val visitorStateSpec = StateSpec
          .function(mapVisitorsStateFunc)
          .timeout(Minutes(120))

      val hll = new HyperLogLogMonoid(12)
      val statefulVisitorsByProduct = activityStream.map(a => {
        ((a.product, a.timestamp_hour), hll(a.visitor.getBytes))
      }).mapWithState(visitorStateSpec)

      val visitorStateSnapshot= statefulVisitorsByProduct.stateSnapshots()
      visitorStateSnapshot
          .reduceByKeyAndWindow(
            (a, b) => b,
            (x, y) => x,
            Seconds(30 / 4 * 4)
          ) // Only saves or expose snapshot every x seconds x in this case is 30
        /******* commented for inserting data to cassandra ----- .foreachRDD(rdd => rdd*/
        .map(sr => VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
          .saveToCassandra("lambda","stream_visitors_by_product")
            /******
              * commented for inserting data to Cassandra as part of last section
              * .toDF()
            .registerTempTable("VisitorsByProduct")
          )*/


      //statefulActivityByProduct.print(10)

      ssc
    }
    val ssc = getStreamingContext(streamingApp, sc, batchDuration)

    ssc.start()
    ssc.awaitTermination()
  }

}
