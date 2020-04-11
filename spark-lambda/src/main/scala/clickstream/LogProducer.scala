package clickstream

import java.io.FileWriter
import java.util.Properties

import config.Settings
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import scala.util.Random

/**
  * extends App, makes the class runnable without defining main function
  * */
object LogProducer extends App {
  // WebLog config
  val wlc = Settings.WebLogGen

  val Products = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/products.csv")).getLines().toArray
  val Referrers = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/referrers.csv")).getLines().toArray
  val Visitors = (0 to wlc.visitors).map("Visitor-" + _)
  val Pages = (0 to wlc.pages).map("Page-" + _)

  val rnd = new Random()

  val props = new Properties()

  // Configurations
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  // Defines whether produce is fireNforget, just leader or all
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "WebLogProducer")

  // Instantiation
  val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](props)
  println("Kafka Producer Partitions for topic : " + kafkaProducer.partitionsFor(wlc.kafkaTopic))
  /** Kafka end */

  val filePath = wlc.filePath
  val destPath = wlc.destPath

  for (fileCount <- 1 to wlc.numberOfFiles) {


    //val fw = new FileWriter(filePath, true)

    // introduce a bit of randomness to time increments for demo purposes
    val incrementTimeEvery = rnd.nextInt(math.min(wlc.records, 100) - 1) + 1

    var timestamp = System.currentTimeMillis()
    var adjustedTimestamp = timestamp

    for (iteration <- 1 to wlc.records) {
      adjustedTimestamp = adjustedTimestamp + ((System.currentTimeMillis() - timestamp) * wlc.timeMultiplier)
      timestamp = System.currentTimeMillis() // move all this to a function
      val action = iteration % (rnd.nextInt(200) + 1) match {
        case 0 => "purchase"
        case 1 => "add_to_cart"
        case _ => "page_view"
      }
      val referrer = Referrers(rnd.nextInt(Referrers.length - 1))
      val prevPage = referrer match {
        case "Internal" => Pages(rnd.nextInt(Pages.length - 1))
        case _ => ""
      }
      val visitor = Visitors(rnd.nextInt(Visitors.length - 1))
      val page = Pages(rnd.nextInt(Pages.length - 1))
      val product = Products(rnd.nextInt(Products.length - 1))

      val line = s"$adjustedTimestamp\t$referrer\t$action\t$prevPage\t$visitor\t$page\t$product\n"

      /** Kafka start */
      val producerRecord = new ProducerRecord(wlc.kafkaTopic, line)
      kafkaProducer.send(producerRecord)
      /** Kafka end */

      if (iteration % incrementTimeEvery == 0) {
        println(s"Sent $iteration messages!")
        val sleeping = rnd.nextInt(1500)
        println(s"Sleeping for $sleeping ms")
        Thread sleep sleeping
      }

    }
    // commented because of kafka based imple -- fw.close()

    /** commented because of kafka based impl

    val outputFile = FileUtils.getFile(s"${destPath}data_$timestamp")
    println(s"Moving produced data to $outputFile")
    FileUtils.moveFile(FileUtils.getFile(filePath), outputFile)

      */
    val sleeping = 2000
    println(s"Sleeping for ${sleeping} ms")
  }

  kafkaProducer.close()
}
