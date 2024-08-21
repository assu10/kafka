package com.assu.study.chap04.standalone;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

// 컨슈머 그룹없이 컨슈머 스스로가 특정 토픽의 모든 파티션을 할당한 뒤 메시지를 읽고 처리
@Slf4j
public class StandaloneConsumer {
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "CountryCounter");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    List<TopicPartition> partitions = new ArrayList<>();
    // 카프카 클러스터에 해당 토픽에 대해 사용 가능한 파티션들을 요청
    // 만일 특정 파티션의 레코드만 읽어올거면 생략해도 됨
    List<PartitionInfo> partitionInfos = consumer.partitionsFor("topic");

    if (partitions != null) {
      for (PartitionInfo partitionInfo : partitionInfos) {
        partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
      }

      // 읽고자 하는 파티션이 있다면 해당 목록에 `assign()` 으로 추가
      consumer.assign(partitions);

      Duration timeout = Duration.ofMillis(100);

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
        }
        consumer.commitSync();
      }
    }
  }
}
