package com.assu.study.chap04;

import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerSample {
  // 컨슈머 생성 예시
  public void simpleConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "CountryCounter");

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      // 하나의 토픽 이름만으로 목록 생성
      consumer.subscribe(Collections.singletonList("customerCountries"));

      // test 가 들어간 모든 토픽 구독
      consumer.subscribe(Pattern.compile("test.*"));
    }
  }
}
