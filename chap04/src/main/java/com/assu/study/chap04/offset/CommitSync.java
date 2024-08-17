package com.assu.study.chap04.offset;

import java.time.Duration;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

// 가장 최근의 메시지 배치를 처리한 뒤 commitSync() 를 호출하여 오프셋 커밋
@Slf4j
public class CommitSync {
  public static void main(String[] args) {
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

        try {
          // 현재 배치의 모든 레코드에 대한 '처리'가 완료되면 추가 메시지를 폴링하기 전에 commitSync() 를 호출해서
          // 해당 배치의 마지막 오프셋 커밋
          consumer.commitSync();
        } catch (CommitFailedException e) {
          log.error("commit failed: {}", e.getMessage());
        }
      }
    }
  }
}
