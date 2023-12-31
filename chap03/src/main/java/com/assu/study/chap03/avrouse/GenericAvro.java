package com.assu.study.chap03.avrouse;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class GenericAvro {
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

    // 에이브로가 자동 생성한 객체를 사용하지 않기 때문에 에이브로 스키마를 직접 지정
    String schemaString = """
        {
          "namespace": "avrogeneric.avro",
          "type": "record",
          "name": "AvroGeneric",
          "fields": [
            {
              "name": "id",
              "type": "int"
            },
            {
              "name": "name",
              "type": "string"
            },
            {
              "name": "email",
              "type": [
                "null",
                "string"
              ],
              "default": null
            }
          ]
        }
        """;

    Producer<String, GenericRecord> producer = new KafkaProducer<>(props);
    String topic = "customerContacts";

    // 에이브로 객체 초기화
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(schemaString);

    // 10 번 진행
    for (int i = 0; i < 10; i++) {
      String name = "sampleName" + i;
      String email = "sampleEmail" + i;

      GenericRecord customer = new GenericData.Record(schema);
      customer.put("id", i);
      customer.put("name", name);
      customer.put("email", email);

      // ProducerRecord 의 밸류값에 직접 지정한 스키마와 데이터가 포함된 GenericRecord 가 들어감
      ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, name, customer);

      // 헤더 추가
      record.headers().add("privacy", "TEST".getBytes(StandardCharsets.UTF_8));

      // 객체 전송 후엔 시리얼라이저가 레코드에서 스키마를 얻어오고, 스키마를 스키마 레지스트리에 저장하고,
      // 객체 데이터를 직렬화하는 과정을 알아서 함
      producer.send(record);
    }
  }
}
