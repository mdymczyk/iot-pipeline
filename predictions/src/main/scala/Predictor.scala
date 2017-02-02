import java.util.Properties

import hex.genmodel.GenModel
import io.confluent.kafka.serializers.KafkaJsonSerializer
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

object Predictor {

  val modelClassName = "DeepLearning_model_R_1485934704251_5"
  val sensorTopic = "/streams/sensor:sensor1"
  val predictionTopic = "/streams/sensor:sensor-state-test"
  val producer: KafkaProducer[String, String] = makeProducer()

  var headers = Array("LinAccX..g.","LinAccY..g.","LinAccZ..g.")
  var features: Int = headers.length

  var threshold = 4.234385e-05

  private def makeProducer() = {
    val props = new Properties()

    props.put("key.serializer", classOf[KafkaJsonSerializer[String]].getCanonicalName)
    props.put("value.serializer", classOf[KafkaJsonSerializer[String]].getCanonicalName)

    new KafkaProducer[String, String](props)
  }

  def main(args: Array[String]): Unit = {
    if(args.length == 1) {
      threshold = args(0).toDouble
    }
    if(args.length == 2) {
      headers = args(1).split(",")
      features = headers.length
    }

    val consumer = new MapRStreamsConsumerFacade(sensorTopic)
    consumer.prepareSetup()
    println("Prepared")
    consumer.open()
    println("Opened")
    poll(consumer)
    producer.close()
    consumer.close()
  }

  private def pushPrediction(label: String) = {
    println(s"Pushing prediction $label")

    producer.send(
      new ProducerRecord[String, String](
        predictionTopic,
        "state",
        label
      )
    )
  }

  import scala.collection.JavaConversions._
  def poll(consumer: MapRStreamsConsumerFacade): Unit = {
    val rb: RingBuffer[Int] = new RingBuffer(100)

    val rawModel: GenModel = Class.forName(modelClassName).newInstance().asInstanceOf[GenModel]
    while(true) {
      val commitMap = new mutable.LinkedHashMap[TopicPartition, OffsetAndMetadata]()

      val records: ConsumerRecords[String, String] = consumer.poll()
      println("Polled " + records.count())

      for(record: ConsumerRecord[String, String] <- records) {
        val split = record.value().replaceAll("\"", "").split(",")
        if(split.length >= features) {
          val preds = Array.fill[Double](features){0}
          val input = split.takeRight(features).map(_.toDouble)
          val pred = rawModel.score0(input, preds)

          val rmse = input.zip(pred).map{ case(i,p) =>  (p-i)*(p-i)}.sum/features

          val label = if (rmse > threshold) 1 else 0

          rb.+=(label)

          if(rb.sum >= 20) {
            pushPrediction("1")
          } else {
            pushPrediction("0")
          }
        }
      }

      if (commitMap.nonEmpty) {
        consumer.commit(commitMap.toMap[TopicPartition, OffsetAndMetadata])
      }
    }
  }

}
