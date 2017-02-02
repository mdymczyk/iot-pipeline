import java.util.Properties

import hex.genmodel.GenModel
import hex.genmodel.easy.prediction.BinomialModelPrediction
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import scala.util.parsing.json.JSON

/**
  * Created by mateusz on 2017/01/26.
  */
object Predictor {

  val modelClassName = "autoencoder"
  val sensorTopic = "/streams/sensor:sensor1"
  val predictionTopic = "/streams/sensor:sensor-state-test"
  val producer: KafkaProducer[String, String] = makeProducer()

  private def makeProducer() = {
    val props = new Properties()

    // TODO check properties
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](props)
  }

  def main(args: Array[String]): Unit = {
    val consumer = new MapRStreamsConsumerFacade(sensorTopic)
    consumer.prepareSetup()
    println("Prepared")
    consumer.open()
    println("Opened")
    poll(consumer)
    producer.close()
    consumer.close()
  }

  private def record2row(value: String) = {
    val row: RowData = new RowData()
    value.split(",").zipWithIndex.map { case(idx, reading) =>
      row.put(idx, reading)
    }
    row
  }

  private def pushPrediction(label: String) = {
    producer.send(
      new ProducerRecord[String, String](
        predictionTopic,
        "state",
        label)
    )
  }

  import scala.collection.JavaConversions._
  def poll(consumer: MapRStreamsConsumerFacade): Unit = {
//    val rawModel: GenModel = Class.forName(modelClassName).newInstance().asInstanceOf[GenModel]
//    val model: EasyPredictModelWrapper = new EasyPredictModelWrapper(rawModel)
    while(true) {
      println("Polling")
      val commitMap = new mutable.LinkedHashMap[TopicPartition, OffsetAndMetadata]()

      val records: ConsumerRecords[String, String] = consumer.poll()
      println("Polled " + records.count())

      for(record: ConsumerRecord[String, String] <- records) {
        println(record.value())
        val rowData: RowData = record2row(record.value())
//        val pred: BinomialModelPrediction = model.predictBinomial(rowData)
//        pushPrediction(pred.label)
        pushPrediction("0")
      }

      if (commitMap.nonEmpty) {
        consumer.commit(commitMap.toMap[TopicPartition, OffsetAndMetadata])
      }
    }
  }

}
