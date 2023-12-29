package com.assu.study.chap03;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CustomerSerializer implements Serializer<Customer> {
  /**
   * customerId: 4 byte int
   * customerName length: 4 byte int, UTF-8
   * customerName: N bytes, UTF-8
   */
  @Override
  public byte[] serialize(String topic, Customer data) {
    try {
      byte[] serializedName;
      int stringSize;
      if (data == null) {
        return null;
      } else {
        if (data.getCustomerName() != null) {
          serializedName = data.getCustomerName().getBytes(StandardCharsets.UTF_8);
          stringSize = serializedName.length;
        } else {
          serializedName = new byte[0];
          stringSize = 0;
        }
      }

      ByteBuffer buf = ByteBuffer.allocate(4 + 4 + stringSize);
      buf.putInt(data.getCustomerId());
      buf.putInt(stringSize);
      buf.put(serializedName);

      return buf.array();
    } catch (Exception e) {
      throw new SerializationException("Error when serializing Customer to byte[] ", e);
    }
  }

  @Override
  public void configure(Map configs, boolean isKey) {
    // nothing to configure
  }

  @Override
  public void close() {
    // nothing to close
  }
}
