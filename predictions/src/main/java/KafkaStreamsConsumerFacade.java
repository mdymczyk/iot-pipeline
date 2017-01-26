import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public interface KafkaStreamsConsumerFacade<K, V> extends AutoCloseable {
  void prepareSetup();

  int partitionCount();

  void open();

  void close();

  String topic();

  ConsumerRecords<K, V> poll();

  void commit(Map<TopicPartition, OffsetAndMetadata> commitMap);
}
