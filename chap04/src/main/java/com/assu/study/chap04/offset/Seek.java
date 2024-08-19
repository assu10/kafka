package com.assu.study.chap04.offset;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Seek {
  // 모든 파티션의 현재 오프셋을 특정한 시각에 생성된 레코드의 오프셋으로 설정
  public void setOffset() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "CustomerCountry");

    Duration timeout = Duration.ofMillis(100);
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

    Long oneHourEarlier =
        Instant.now().atZone(ZoneId.systemDefault()).minusHours(1).toEpochSecond();

    // consumer.assignment() 로 얻어온 컨슈머에 할당된 모든 파티션에 대해
    // 컨슈머를 되돌리고자 하는 타임스탬프 값을 담은 맵 생성
    Map<TopicPartition, Long> partitionTimestampMap =
        consumer.assignment().stream().collect(Collectors.toMap(tp -> tp, tp -> oneHourEarlier));

    // 각 타임스탬프에 대한 오프셋 받아옴
    // offsetsForTimes() 는 브로커에 요청을 보내서 타임스탬프 인덱스에 저장된 오프셋을 리턴함
    Map<TopicPartition, OffsetAndTimestamp> offsetMap =
        consumer.offsetsForTimes(partitionTimestampMap);

    // 각 파티션의 오프셋을 앞 단계에서 리턴된 오프셋으로 재설정
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetMap.entrySet()) {
      consumer.seek(entry.getKey(), entry.getValue().offset());
    }
  }
}
