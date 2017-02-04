import java.util.Properties

import hex.genmodel.GenModel
import io.confluent.kafka.serializers.KafkaJsonSerializer
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import scala.io.Source

object Predictor {

  val modelClassName = "iot_dl"
  val sensorTopic = "/streams/sensor:sensor1"
  val predictionTopic = "/streams/sensor:sensor-state-test"
  val producer: KafkaProducer[String, Int] = makeProducer()

  var state = 0

  var window: Int = 200
  var predictionCacheSize: Int = window*5
  var failureRate: Double = 1.0/(5.0*predictionCacheSize.toDouble)

  // Settable
  var headers = Array("LinAccX..g.","LinAccY..g.","LinAccZ..g.")
  var features: Int = headers.length
  var timer: Double = window * 10

  var threshold: Double = {
    val props = new Properties()
    props.load(Source.fromURL(getClass.getResource("/dl.properties")).bufferedReader())
    val t = props.get("threshold").toString.toDouble
    println(s"Setting threshold to $t")
    t
  }

  private def makeProducer() = {
    val props = new Properties()

    props.put("key.serializer", classOf[KafkaJsonSerializer[String]].getCanonicalName)
    props.put("value.serializer", classOf[KafkaJsonSerializer[String]].getCanonicalName)

    new KafkaProducer[String, Int](props)
  }

  def main(args: Array[String]): Unit = {
    for(arg <- args) {
      val kv = arg.split("=")
      if(kv(0) == "threshold") {
        print(s"Setting threshold to ${kv(1).toDouble} from the command line.")
        threshold = kv(1).toDouble
      } else if(kv(0) == "features") {
        print(s"Setting headers to [${kv(1)}]")
        headers = kv(1).split(",")
        features = headers.length
      } else if(kv(0) == "failureRate") {
        print(s"Setting failureRate to [${kv(1)}]")
        failureRate = kv(1).toDouble
      } else if(kv(0) == "timer") {
        print(s"Setting timer to [${kv(1)}]")
        timer = kv(1).toDouble
      } else if(kv(0) == "predictionCacheSize") {
        print(s"Setting predictionCacheSize to [${kv(1)}]")
        predictionCacheSize = kv(1).toInt
        failureRate = 1.0/(5.0*predictionCacheSize.toDouble)
      }
    }

    // Sender
    new Thread() {
      override def run(): Unit = {
        while(true) {
          Thread.sleep(timer.toLong)
          pushPrediction(state)
        }
      }
    }.start()

    val consumer = new MapRStreamsConsumerFacade(sensorTopic)
    try {
      consumer.prepareSetup()
      println("Prepared")
      consumer.open()
      println("Opened")
      poll(consumer)
    } finally {
      producer.close()
      consumer.close()
    }
  }

  private def pushPrediction(label: Int) = {
    println(s"Pushing prediction $label")

    producer.send(
      new ProducerRecord[String, Int](
        predictionTopic,
        "state",
        label
      )
    )
  }

  import scala.collection.JavaConversions._

  def poll(consumer: MapRStreamsConsumerFacade): Unit = {
    val fullWindow = features * window
    val inputRB: RingBuffer[Double] = new RingBuffer(fullWindow)

    val rawModel: GenModel = Class.forName(modelClassName).newInstance().asInstanceOf[GenModel]

    val rb: RingBuffer[Int] = new RingBuffer(predictionCacheSize)
    while(true) {
      val commitMap = new mutable.LinkedHashMap[TopicPartition, OffsetAndMetadata]()

      val records: ConsumerRecords[String, String] = consumer.poll()
      println("Polled " + records.count())

      for(record: ConsumerRecord[String, String] <- records) {
        val split = record.value().replaceAll("\"", "").split(",")
        if(split.length >= features) {
          val input = split.takeRight(features).map(_.toDouble)
          for(i <- input) {
            inputRB.+=(i)
          }

          if(inputRB.length == fullWindow) {
            val preds = Array.fill[Double](fullWindow){0}
            val pred = rawModel.score0(inputRB.toArray, preds)

            val rmse = inputRB.zip(pred).map { case (i, p) => (p - i) * (p - i) }.sum / (fullWindow).toDouble

            val label = if (rmse > threshold) 1 else 0

            rb.+=(label)

            if ((rb.sum.toDouble / rb.length.toDouble) >= failureRate) {
              state = 1
            } else {
              state = 0
            }
          }
        }
      }

      if (commitMap.nonEmpty) {
        consumer.commit(commitMap.toMap[TopicPartition, OffsetAndMetadata])
      }
    }
  }

}
