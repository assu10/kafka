package com.assu.study.chap04.deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

// Customer 클래스를 위한 커스텀 디시리얼라이저

// 컨슈머에서도 Customer 클래스 필요
// 프로듀서에 사용되는 클래스 및 시리얼라이저는 컨슈머 애플리케이션에서도 같은 것이 사용되어야 함
// 같은 데이터를 공유해서 사용하는 컨슈머와 프로듀서의 수가 많은 조직에서는 꽤나 어려운 일임
public class CustomerDeserializer implements Deserializer<Customer> {
  @Override
  public Customer deserialize(String topic, byte[] data) {
    int id;
    int nameSize;
    String name;

    try {
      if (data == null) {
        return null;
      }

      if (data.length < 8) {
        throw new SerializationException(
            "Size of data received by deserializer is shorter than expected.");
      }

      ByteBuffer buf = ByteBuffer.wrap(data);
      id = buf.getInt();
      nameSize = buf.getInt();

      byte[] nameBytes = new byte[nameSize];
      buf.get(nameBytes);
      name = new String(nameBytes, StandardCharsets.UTF_8);

      // 고객 id 와 이름을 바이트 배열로부터 꺼내온 후 필요로 하는 객체 생성
      return new Customer(id, name);
    } catch (Exception e) {
      throw new SerializationException("Error when deserializing byte[] to Customer: ", e);
    }
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // nothing to configure
  }

  @Override
  public void close() {
    // nothing to close
  }
}
