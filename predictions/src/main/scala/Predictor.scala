import java.io.IOException

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

/**
  * Created by mateusz on 2017/01/26.
  */
object Predictor {

  def main(args: Array[String]): Unit = {
    val consumer = new MapRStreamsConsumerFacade("sensor-state-test")
    consumer.prepareSetup()
    consumer.open()
    poll(consumer)
  }

  def poll(consumer: MapRStreamsConsumerFacade): Unit = {
    val commitMap = new mutable.LinkedHashMap[TopicPartition, OffsetAndMetadata]();

    val records: ConsumerRecords[String, String] = consumer.poll()

    if (commitMap.nonEmpty) {
      consumer.commit(commitMap.toMap.toJava)
    }
  }

}
