package com.assu.study.chap03.avrouse;

import avrocustomer.avro.AvroCustomer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class AvroProducer {
  public static void main(String[] args) {
    Properties props = new Properties();

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    // 에이브로를 사용해서 객체를 직렬화하기 위해 KafkaAvroSerializer 사용
    // KafkaAvroSerializer 는 객체 뿐 아니라 기본형 데이터도 처리 가능
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

    // 에이브로 시리얼라이저의 설정 매개변수 설정
    // 프로듀서가 시리얼라이저에 넘겨주는 값으로, 스키마를 저장해놓은 위치를 가리킴 (= 스키마 레지스트리의 url 지정)
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://sample.schemaurl");

    String topic = "customerContacts";

    // AvroCustomer 는 POJO 가 아니라 에이브로의 코드 생성 기능을 사용해서 스키마로부터 생성된 에이브로 특화 객체임
    // 에이브로 시리얼라이저는 POJO 객체가 아닌 에이브로 객체만을 직렬화 가능
    Producer<String, AvroCustomer> producer = new KafkaProducer<>(props);

    // 10 번 진행
    for (int i = 0; i < 10; i++) {
      AvroCustomer customer = new AvroCustomer();
      System.out.println("Avro Customer " + customer.toString());

      // 밸류 타입이 AvroCustomer 인 ProducerRecord 객체를 생성하여 전달
      ProducerRecord<String, AvroCustomer> record = new ProducerRecord<>(topic, customer.getName(), customer);

      // AvroCustomer 객체 전송 후엔 KafkaAvroSerializer 가 알아서 해줌
      producer.send(record);
    }
  }
}
