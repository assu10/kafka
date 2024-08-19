package com.assu.study.chap04.deserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

@Slf4j
public class ConsumerUseCustomerDeserializer {
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        Serdes.String().deserializer().getClass().getName());
    // CustomerDeserializer 클래스 설정
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerDeserializer.class.getName());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "CustomerCountry");

    Duration timeout = Duration.ofMillis(100);

    KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList("topic"));

    while (true) {
      ConsumerRecords<String, Customer> records = consumer.poll(timeout);
      for (ConsumerRecord<String, Customer> record : records) {
        log.info(
            "current customer Id: {}, customer name: {}",
            record.value().getCustomerId(),
            record.value().getCustomerName());
      }
      consumer.commitSync();
    }
  }
}
