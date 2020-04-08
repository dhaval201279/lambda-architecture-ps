package streaming

import domain.{Activity, ActivityByProduct, VisitorsByProduct}
import org.apache.spark.SparkContext
import functions._
import org.apache.spark.streaming._
import utils.ScalaUtils._
import com.twitter.algebird.HyperLogLogMonoid

object StreamingJob {
  def main(args: Array[String]): Unit = {
    // Setup Spark Context
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)

    import sqlContext.implicits._

    val batchDuration = Seconds(4)
    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)

      val inputPath = isIDE match {
        case true => "file:///H:/Dhaval/Tech/pluralsight-lambda-arch/Boxes/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
        case false => "file:///vagrant/input"
      }

      val textDStream = ssc.textFileStream(inputPath)
      val activityStream = textDStream.transform(input => {
        input.flatMap{ line =>
          val record = line.split("\\t")
          val MS_IN_HOUR = 1000 * 60 * 60
          if (record.length == 7)
            Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
          else
            None
        }
      }).cache()

      // from below code variable initialization is for using mapWithState
      val activityStateSpec = StateSpec
        .function(mapActivityStateFunc)
        .timeout(Seconds(30))
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
        .foreachRDD(rdd => rdd.map(
        sr => ActivityByProduct(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3)
      )
        .toDF()
        .registerTempTable("ActivityByProduct")
      )
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
          .foreachRDD(rdd => rdd.map(
            sr => VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate)
          )
            .toDF()
            .registerTempTable("VisitorsByProduct")
          )


      //statefulActivityByProduct.print(10)

      ssc
    }
    val ssc = getStreamingContext(streamingApp, sc, batchDuration)

    ssc.start()
    ssc.awaitTermination()
  }

}
