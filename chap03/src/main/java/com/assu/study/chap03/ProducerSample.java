package com.assu.study.chap03;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerSample {
  // 메시지를 전송하는 간단한 예시
  public void simpleProducer() {
    Properties KafkaProps = new Properties();
    KafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    KafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    KafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    Producer<String, String> producer = new KafkaProducer<>(KafkaProps);
    // ProducerRecord 객체 생성
    ProducerRecord<String, String> record = new ProducerRecord<>("Topic_Country", "Key_Products", "Value_Korea");
    try {
      // ProducerRecord 객체를 전송하기 위해 프로듀서 객체의 send() 사용
      // 메시지는 버퍼에 저장되었다가 별도 스레드에 의해 브로커로 전달됨
      // Future<RecordMetadata> 를 리턴하지만 여기선 리턴값을 무시하기 때문에 메시지 전송의 성공 여부 모름
      // 따라서 실제 이런 식으로는 잘 사용하지 않음
      producer.send(record);
    } catch (Exception e) {
      // 프로듀서가 카프카로 메시지를 보내기 전 발생하는 에러 캐치
      e.printStackTrace();
    }
  }

  // 동기적으로 메시지 전송
  public void syncProducer() {
    Properties KafkaProps = new Properties();
    KafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    KafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    KafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    Producer<String, String> producer = new KafkaProducer<>(KafkaProps);
    // ProducerRecord 객체 생성
    ProducerRecord<String, String> record = new ProducerRecord<>("Topic_Country", "Key_Products", "Value_Korea");
    try {
      // 카프카로부터 응답이 올 때까지 대기하기 위해 Future.get() 메서드 사용
      // Future.get() 메서드는 레코드가 카프카로 성공적으로 전송되지 않았을 경우 예외 발생시킴
      // 에러가 발생하지 않으면 RecordMetadata 객체 리턴
      // RecordMetadata 에서 메시지가 쓰여진 오프셋과 다른 메타데이터 조회 가능
      producer.send(record).get();
    } catch (Exception e) {
      // 프로듀서가 카프카로 메시지를 보내기 전이나 전송하는 도중 발생하는 에러 캐치
      e.printStackTrace();
    }
  }

  // 비동기적으로 메시지 전송하면서 콜백으로 에러 처리
  public void AsyncProducerWithCallback() {
    Properties KafkaProps = new Properties();
    KafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    KafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    KafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    Producer<String, String> producer = new KafkaProducer<>(KafkaProps);
    // ProducerRecord 객체 생성
    ProducerRecord<String, String> record = new ProducerRecord<>("Topic_Country", "Key_Products", "Value_Korea");
    // 레코드 전송 시 콜백 객체 전달
    producer.send(record, new AsyncProducerCallback());
  }

  // 비동기적으로 메시지 전송하면서 콜백으로 에러 처리
  // 콜백을 사용하려면 Callback 인터페이스를 구현하는 클래스 필요
  private class AsyncProducerCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e != null) {
        // 카프카가 에러를 리턴하면 onCompletion() 은 null 이 아닌 Exception 객체를 받음
        // 실제론는 아래처럼 출력 뿐이 아니라 확실한 에러 처리 함수가 필요함
        e.printStackTrace();
      }
    }
  }
}
