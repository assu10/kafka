package com.assu.study.chap04.offset;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class CommitAsync {
  public void commitAsync() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "CustomerCountry");

    Duration timeout = Duration.ofMillis(100);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        for (ConsumerRecord<String, String> record : records) {
          // 여기서는 로그가 출력되면 처리가 끝나는 것으로 간주함
          log.info(
              "topic: {}, partition: {}, offset: {}, customer: {}, country: {}",
              record.topic(),
              record.partition(),
              record.offset(),
              record.key(),
              record.value());
        }
        // 마지막 오프셋을 커밋하고 처리 작업을 계속함
        consumer.commitAsync();
      }
    }
  }

  public void commitAsyncWithCallback() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "CustomerCountry");

    Duration timeout = Duration.ofMillis(100);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        for (ConsumerRecord<String, String> record : records) {
          // 여기서는 로그가 출력되면 처리가 끝나는 것으로 간주함
          log.info(
              "topic: {}, partition: {}, offset: {}, customer: {}, country: {}",
              record.topic(),
              record.partition(),
              record.offset(),
              record.key(),
              record.value());
        }
        // 마지막 오프셋을 커밋하고 처리 작업을 계속함
        // 하지만, 커밋이 실패할 경우 실패가 났다는 사실과 함께 오프셋을 로그에 남김
        consumer.commitAsync(
            new OffsetCommitCallback() {
              @Override
              public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                if (e != null) {
                  log.error("Commit failed of offsets: {}", e.getMessage());
                }
              }
            });
      }
    }
  }

  // 컨슈머 종료 직전에 동기적 커밋과 비동기적 커밋 함께 사용
  public void commitSyncAndAsyncBeforeClosingConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "CustomerCountry");

    Duration timeout = Duration.ofMillis(100);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      boolean closing = false; // TODO: closing 업데이트
      while (!closing) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        for (ConsumerRecord<String, String> record : records) {
          // 여기서는 로그가 출력되면 처리가 끝나는 것으로 간주함
          log.info(
              "topic: {}, partition: {}, offset: {}, customer: {}, country: {}",
              record.topic(),
              record.partition(),
              record.offset(),
              record.key(),
              record.value());
        }
        // 정상적인 상황에서는 비동기 커밋 사용
        // 더 빠를 뿐더러 커밋이 실패해도 다음 커밋이 성공할 수 있음
        consumer.commitAsync();
      }
      // 컨슈머를 닫는 상황에서는 '다음 커밋' 이 없으므로 commitSync() 를 호출하여
      // 커밋의 성공하면 종료, 회복 불가능한 에러가 발생할 때까지 재시도함
      consumer.commitSync();
    } catch (Exception e) {
      log.error("Unexpected error: {}", e.getMessage());
    }
  }
}
