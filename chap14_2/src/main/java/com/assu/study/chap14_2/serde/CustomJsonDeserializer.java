package com.assu.study.chap14_2.serde;

import com.google.gson.Gson;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

public class CustomJsonDeserializer<T> implements Deserializer<T> {
  private Gson gson = new Gson();
  private Class<T> deserializedClass;

  public CustomJsonDeserializer(Class<T> deserializedClass) {
    this.deserializedClass = deserializedClass;
  }

  public CustomJsonDeserializer() {}

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> map, boolean b) {
    if (deserializedClass == null) {
      deserializedClass = (Class<T>) map.get("serializedClass");
    }
  }

  @Override
  public T deserialize(String s, byte[] bytes) {
    if (bytes == null) {
      return null;
    }

    return gson.fromJson(new String(bytes), deserializedClass);
  }

  @Override
  public void close() {}
}
