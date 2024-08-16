package com.assu.study.chap04.polling;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.configurationprocessor.json.JSONObject;

@Slf4j
public class PollingLoop {
  static Map<String, Integer> custCountryMap = new HashMap<>();

  static {
    custCountryMap.put("assu", 1);
    custCountryMap.put("silby", 1);
  }

  public void polling() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "CustomerCountry");

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

      // timeout 은 100ms
      Duration timeout = Duration.ofMillis(100);

      // 컨슈머 애플리케이션은 보통 게속해서 카프카에 추가 데이터를 폴링함
      // (나중에 루프를 탈출하여 컨슈머를 깔끔하게 닫는 법에 대해 알아볼 예정)
      while (true) {
        // 컨슈머가 카프카를 계속해서 폴링하지 않으면 죽은 것으로 간주되어 이 컨슈머가 읽어오고 있던 파티션들은
        // 처리를 계속 하기 위해 그룹 내의 다른 컨슈머에게 넘김
        // poll() 에 전달하는 timeout 은 컨슈머 버퍼에 데이터가 없을 경우 poll() 이 블록될 수 있는 최대 시간임
        // 만일 timeout 이 0 으로 지정되거나, 버퍼 안에 이미 레코드가 준비되어 있을 경우 poll() 은 즉시 리턴됨
        // 그게 아니면 지정된 ms 만큼 기다림
        ConsumerRecords<String, String> records = consumer.poll(timeout);

        // poll() 은 레코드들이 저장된 List 반환
        // 각 레코드들에는 토픽, 파티션, 파티션에서의 오프셋, 키, 밸류값이 들어가있음
        for (ConsumerRecord<String, String> record : records) {
          log.info(
              "topic: {}, partition: {}, offset: {}, customer: {}, country: {}",
              record.topic(),
              record.partition(),
              record.offset(),
              record.key(),
              record.value());

          int updatedCount = 1;
          if (custCountryMap.containsKey(record.value())) {
            updatedCount = custCountryMap.get(record.value()) + 1;
          }
          custCountryMap.put(record.value(), updatedCount);

          // 처리가 끝나면 결과물을 데이터 저장소에 쓰거나, 이미 저장된 레코드 갱신
          JSONObject json = new JSONObject(custCountryMap);
          log.info("json: {}", json);
        }
      }
    }
  }
}
