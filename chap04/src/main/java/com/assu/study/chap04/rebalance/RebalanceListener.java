package com.assu.study.chap04.rebalance;

import java.time.Duration;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

// 파티션이 해제되기 직전에 동기적 커밋과 비동기적 커밋 함께 사용하여 마지막 오프셋 커밋
@Slf4j
public class RebalanceListener {
  static Properties props = new Properties();

  static {
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "CustomerCountry");
  }

  private Duration timeout = Duration.ofMillis(100);
  private KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
  private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

  public void commitSyncAndAsyncBeforePartitionRevoked() {
    try {
      // subscribe() 호출 시 ConsumerRebalanceListener 를 인수로 지정하여 컨슈머가 호출할 수 있도록 해줌
      consumer.subscribe(Collections.singletonList("testTopic"), new HandleRebalance());

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        for (ConsumerRecord<String, String> record : records) {
          log.info(
              "topic: {}, partition: {}, offset: {}, customer: {}, country: {}",
              record.topic(),
              record.partition(),
              record.offset(),
              record.key(),
              record.value());

          currentOffsets.put(
              new TopicPartition(record.topic(), record.partition()),
              new OffsetAndMetadata(record.offset() + 1, null));
          consumer.commitAsync(currentOffsets, null); // 여기선 callback 을 null 로 처리
        }
      }
    } catch (WakeupException e) {
      // ignore, we're closing
    } catch (Exception e) {
      log.error("Unexpected error: {}", e.getMessage());
    } finally {
      try {
        consumer.commitSync(currentOffsets);
      } finally {
        consumer.close();
        log.info("Closed consumer and we are done.");
      }
    }
  }

  // ConsumerRebalanceListener 구현
  private class HandleRebalance implements ConsumerRebalanceListener {
    // 여기서는 컨슈머가 새 파티션을 할당받았을 때 아무것도 할 필요가 없기 때문에 바로 메시지를 읽기 시작함
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      log.error("Lost partitions in rebalance. Commiting current offsets: {}", currentOffsets);
      // 리밸런싱 때문에 파티션이 해제될 상황이라면 오프셋 커밋
      // 할당 해제될 파티션의 오프셋 뿐 아니라 모든 파티션에 대한 오프셋 커밋
      // (이 오프셋들은 이미 처리된 이벤트들의 오프셋이므로 문제없음)
      // 리밸런스가 진행되기 전에 모든 오프셋이 확실히 커밋되도록 commitSync() 사용
      consumer.commitSync(currentOffsets);
    }
  }
}
