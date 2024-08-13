package com.assu.study.chap03.serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

// Customer 클래스를 위한 커스텀 시리얼라이저
public class CustomerSerializer implements Serializer<Customer> {

  /**
   * customerId: 4 byte int customerNameLength: 4 byte int, UTF-8 (customerName 이 null 이면 0)
   * customerName: N bytes, UTF-8
   */
  @Override
  public byte[] serialize(String topic, Customer data) {
    try {
      byte[] serializeName;
      int stringSize;
      if (data == null) {
        return null;
      } else {
        if (data.getCustomerName() != null) {
          serializeName = data.getCustomerName().getBytes(StandardCharsets.UTF_8);
          stringSize = serializeName.length;
        } else {
          serializeName = new byte[0];
          stringSize = 0;
        }
      }

      ByteBuffer buf = ByteBuffer.allocate(4 + 4 + stringSize);
      buf.putInt(data.getCustomerId());
      buf.putInt(stringSize);
      buf.put(serializeName);

      return buf.array();
    } catch (Exception e) {
      throw new SerializationException("Error when serializing Customer to byte[]: ", e);
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
