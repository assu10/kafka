package com.assu.study.chap03;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerSample {
  // 메시지를 전송하는 간단한 예시
  public void simpleMessageSend() {
    Properties kafkaProp = new Properties();
    kafkaProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    kafkaProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    kafkaProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    Producer<String, String> producer = new KafkaProducer<>(kafkaProp);

    // ProducerRecord 객체 생성
    ProducerRecord<String, String> record =
        new ProducerRecord<>("Topic_Country", "Key_Product", "Value_France");

    try {
      // ProducerRecord 를 전송하기 위해 프로듀서 객체의 send() 사용
      // 메시지는 버퍼에 저장되었다가 별도 스레드에 의해 브로커로 보내짐
      // send() 메서드는 Future<RecordMetadata> 를 리턴하지만 여기선 리턴값을 무시하기 때문에 메시지 전송의 성공 여부를 모름
      // 따라서 이런 식으로는 잘 사용하지 않음
      producer.send(record);
    } catch (Exception e) {
      // 프로듀서가 카프카로 메시지를 보내기 전 발생하는 에러 캐치
      e.printStackTrace();
    }
  }

  // 동기적으로 메시지 전송
  public void syncMessageSend() {
    Properties kafkaProp = new Properties();
    kafkaProp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    kafkaProp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    kafkaProp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    Producer<String, String> producer = new KafkaProducer<>(kafkaProp);

    // ProducerRecord 객체 생성
    ProducerRecord<String, String> record =
        new ProducerRecord<>("Topic_Country", "Key_Product", "Value_France");

    try {
      // 카프카로부터 응답이 올 때까지 대기하기 위해 Future.get() 메서드 사용
      // Future.get() 메서드는 레코드가 카프카로 성공적으로 전송되지 않았을 경우 예외 발생시킴
      // 에러가 발생하지 않으면 RecordMetadata 객체 리턴
      // 리턴되는 RecordMetadata 로부터 메시지가 쓰여진 오프셋과 다른 메타데이터 조회 가능
      producer.send(record).get();
    } catch (Exception e) {
      // 프로듀서가 카프카로 메시지를 보내기 전이나 전송하는 도중 발생하는 에러 캐치
      e.printStackTrace();
    }
  }
}
