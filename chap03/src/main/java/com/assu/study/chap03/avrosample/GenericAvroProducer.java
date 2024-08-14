package com.assu.study.chap03.avrosample;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class GenericAvroProducer {
  public static void main(String[] args) {
    Properties props = new Properties();

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://sample.schemaurl");

    // 에이브로가 자동 생성한 객체를 사용하지 않기 때문에 에이브로 스키마를 직접 지정
    String schemaString =
        """
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

    for (int i = 0; i < 10; i++) {
      String name = "sampleName" + i;
      String email = "sampleEmail" + i;

      GenericRecord customer = new GenericData.Record(schema);
      customer.put("id", i);
      customer.put("name", name);
      customer.put("email", email);

      // ProducerRecord 의 밸류값에 "직접 지정한 스키마와 데이터"가 포함된 GenericRecord 가 들어감
      ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, name, customer);

      // 헤더 추가
      record.headers().add("privacy-level", "YOLO".getBytes(StandardCharsets.UTF_8));

      // 시리얼라이저는 레코드를 스키마에서 얻어오고, 스키마 레지스트리에 저장하며,
      // 객체 데이터 직렬화하는 과정을 알아서 처리함
      producer.send(record);
    }
  }
}
