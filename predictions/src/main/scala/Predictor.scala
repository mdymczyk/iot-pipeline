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
  val sensorTopic = "sensor1"
  val predictionTopic = "sensor-state-test"
  val producer: KafkaProducer[String, String] = makeProducer()

  private def makeProducer() = {
    val props = new Properties()

    // TODO check properties
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", 0)
    props.put("batch.size", 16384)
    props.put("linger.ms", 1)
    props.put("buffer.memory", 33554432)
    props.put("key.serializer", "org.apache.kafka.common.serializa-tion.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serializa-tion.StringSerializer")

    new KafkaProducer[String, String](props)
  }

  def main(args: Array[String]): Unit = {
    val consumer = new MapRStreamsConsumerFacade(sensorTopic)
    consumer.prepareSetup()
    consumer.open()
    poll(consumer)
    producer.close()
    consumer.close()
  }

  private def record2row(s: String) = {
    val row: RowData = new RowData()
    val jsonRecord = JSON.parseFull(s).get.asInstanceOf[Map[String, AnyRef]]
    jsonRecord.foreach{ case (key, value) => row.put(key, value)}
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

  def poll(consumer: MapRStreamsConsumerFacade): Unit = {
    val rawModel: GenModel = Class.forName(modelClassName).newInstance().asInstanceOf[GenModel]
    val model: EasyPredictModelWrapper = new EasyPredictModelWrapper(rawModel)

    while(true) {
      val commitMap = new mutable.LinkedHashMap[TopicPartition, OffsetAndMetadata]()

      val records: ConsumerRecords[String, String] = consumer.poll()

      for(record: ConsumerRecord[String, String] <- records) {
        val rowData: RowData = record2row(record.value())
        val pred: BinomialModelPrediction = model.predictBinomial(rowData)
        pushPrediction(pred.label)
      }

      if (commitMap.nonEmpty) {
        import scala.collection.JavaConversions._
        consumer.commit(commitMap.toMap)
      }
    }
  }

}
