import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Facade for consumer side API of MapR Streams.
 * Handles multi-threaded use case through reference counting with open() and close().
 */
public class MapRStreamsConsumerFacade implements KafkaStreamsConsumerFacade<String, String> {
  private static final Logger log = LoggerFactory.getLogger(MapRStreamsConsumerFacade.class);

  private static final long DEFAULT_TIMEOUT = 2_500L;

  private final AtomicInteger referenceCount = new AtomicInteger(0);
  private Consumer<String, String> streamsConsumer;
  private String pathTopic;
  private AtomicBoolean initialized = new AtomicBoolean(false);

  public MapRStreamsConsumerFacade(String pathTopic) {
    this.pathTopic = pathTopic;
  }

  private Properties createDefaultProperties() {
    Properties props = new Properties();
    final String topic = pathTopic.substring(pathTopic.indexOf(':'));
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "gid." + topic);
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                      StringDeserializer.class.getCanonicalName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                      StringDeserializer.class.getCanonicalName());
    log.debug("Properties: {}", props);

    return props;
  }

  @Override
  public void prepareSetup() {
    log.debug("prepareSetup() isInitialized={}", initialized.get());
    if (!initialized.getAndSet(true)) {
      streamsConsumer = new KafkaConsumer<>(createDefaultProperties());
      streamsConsumer.subscribe(Collections.singletonList(pathTopic));

      initializeCommitOffset();
      log.debug("prepareSetup() initialization complete!");
    }
  }

  private void initializeCommitOffset() {
    Map<TopicPartition, OffsetAndMetadata> commitMap = new LinkedHashMap<>();

    for (TopicPartition topicPartition : streamsConsumer.assignment()) {
      try {
        streamsConsumer.committed(topicPartition);
      } catch (UnknownTopicOrPartitionException e) {
        commitMap.put(topicPartition, new OffsetAndMetadata(1));
      }
    }
    log.debug("initializeCommitOffset() commitMap.size(): {}", commitMap.size());

    if (commitMap.size() > 0) {
      streamsConsumer.commitSync(commitMap);
    }
  }

  @Override
  public int partitionCount() {
    return streamsConsumer.partitionsFor(pathTopic).size();
  }

  @Override
  public void open() {
    log.debug("open() refs= {}", referenceCount.incrementAndGet());
  }

  @Override
  public void close() {
    if (0 >= referenceCount.decrementAndGet()) {
      streamsConsumer.close();
    }
    log.debug("close() refs= {}", referenceCount.get());
  }

  @Override
  public String topic() {
    return pathTopic;
  }

  @Override
  @SuppressWarnings("unchecked")
  public ConsumerRecords<String, String> poll() {
    return streamsConsumer.poll(DEFAULT_TIMEOUT);
  }

  @Override
  public void commit(Map<TopicPartition, OffsetAndMetadata> commitMap) {
    streamsConsumer.commitSync(commitMap);
  }

  // for testing purposes ONLY!
  void setStreamsConsumer(Consumer<String, String> streamsConsumer) {
    this.streamsConsumer = streamsConsumer;
  }
}
