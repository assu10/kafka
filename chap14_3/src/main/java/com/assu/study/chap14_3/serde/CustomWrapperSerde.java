package com.assu.study.chap14_3.serde;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class CustomWrapperSerde<T> implements Serde<T> {
  private final Serializer<T> serializer;
  private final Deserializer<T> deserializer;

  public CustomWrapperSerde(Serializer<T> serializer, Deserializer<T> deserializer) {
    this.serializer = serializer;
    this.deserializer = deserializer;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    serializer.configure(configs, isKey);
    deserializer.configure(configs, isKey);
  }

  @Override
  public void close() {
    serializer.close();
    deserializer.close();
  }

  @Override
  public Serializer<T> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return deserializer;
  }
}
