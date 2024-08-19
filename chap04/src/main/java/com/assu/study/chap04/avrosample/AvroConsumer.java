package com.assu.study.chap04.avrosample;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import javasessionize.avro.LogLine;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

@Slf4j
public class AvroConsumer {
  private final KafkaConsumer<String, LogLine> consumer;
  private final KafkaProducer<String, LogLine> producer;
  private final String inputTopic;
  private final String outputTopic;
  private Map<String, SessionState> state = new HashMap<>();

  public AvroConsumer(
      String brokers, String groupId, String inputTopic, String outputTopic, String url) {
    this.consumer = new KafkaConsumer<>(createConsumerConfig(brokers, groupId, url));
    this.producer = getProducer(outputTopic, url);
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    // 하드 코딩
    String groupId = "avroSample";
    String inputTopic = "iTopic";
    String outputTopic = "oTopic";
    String url = "http://localhost:8081";
    String brokers = "localhost:9092";

    AvroConsumer avroConsumer = new AvroConsumer(brokers, groupId, inputTopic, outputTopic, url);

    avroConsumer.run();
  }

  private void run() throws ExecutionException, InterruptedException {
    consumer.subscribe(Collections.singletonList(inputTopic));

    log.info("Reading topic: {}", inputTopic);

    Duration timeout = Duration.ofMillis(1000);

    while (true) {
      // 레코드 밸류 타입으로 LogLine 지정
      ConsumerRecords<String, LogLine> records = consumer.poll(timeout);
      for (ConsumerRecord<String, LogLine> record : records) {
        String ip = record.key();
        LogLine event = record.value();

        SessionState oldState = state.get(ip);
        int sessionId = 0;

        if (oldState == null) {
          state.put(ip, new SessionState(event.getTimestamp(), 0));
        } else {
          sessionId = oldState.getSessionId();

          // old timestamp 가 30분 이전의 것이면 새로운 세션 생성
          if (oldState.getLastConnection() < event.getTimestamp() - (30 * 60 * 1000)) {
            sessionId = sessionId + 1;
          }
          SessionState newState = new SessionState(event.getTimestamp(), sessionId);
          state.put(ip, newState);
        }

        event.setSessionid(sessionId);

        log.info("event: {}", event);

        ProducerRecord<String, LogLine> sessionizedEvent =
            new ProducerRecord<>(outputTopic, event.getIp().toString(), event);
        producer.send(sessionizedEvent).get();
      }
      consumer.commitSync();
    }
  }

  private Properties createConsumerConfig(String brokers, String groupId, String url) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // 스키마가 저장된 곳
    // 프로듀서가 등록한 스키마를 컨슈머가 역직렬화하기 위해 사용
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, url);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        Serdes.String().deserializer().getClass().getName());
    // Avro 메시지를 역직렬화하기 위해 KafkaAvroDeserializer 사용
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

    return props;
  }

  private KafkaProducer<String, LogLine> getProducer(String topic, String url) {
    Properties props = new Properties();
    // 여기선 카프카 서버 URI 하드코딩
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, url);

    KafkaProducer<String, LogLine> producer = new KafkaProducer<>(props);
    return producer;
  }
}
