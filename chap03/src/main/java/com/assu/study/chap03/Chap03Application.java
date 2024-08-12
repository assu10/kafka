package com.assu.study.chap03;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Chap03Application {

  public static void main(String[] args) {
    SpringApplication.run(Chap03Application.class, args);

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
}
