package com.assu.study.chap04.list;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.*;

// List<T> 에서 키 값에 대한 시리얼라이저, 디시리얼라이저
public class ListSerializerSample {
  // List<T> 키 값에 대한 시리얼라이저
  public void keySerializer() {
    // ListSerializer 객체 생성
    ListSerializer<?> listSerializer = new ListSerializer<>();

    // 생성한 객체 설정
    Map<String, Object> props = new HashMap<>();

    // 밸류값의 경우엔 default.list.value.serde.inner
    props.put(
        CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class.getName());

    // 밸류값의 경우엔 두 번째 인수가 false
    listSerializer.configure(props, true);

    // 설정된 시리얼라이저 얻어옴
    final Serializer<?> inner = listSerializer.getInnerSerializer();
  }

  // List<T> 키 값에 대한 디시리얼라이저
  public void keyDeserializer() {
    // ListDeserializer 객체 생성
    ListDeserializer<?> listDeserializer = new ListDeserializer<>();

    // 생성한 객체 설정
    Map<String, Object> props = new HashMap<>();

    // 바이트 뭉치를 디시리얼라이즈 한 뒤 ArrayList 객체 형태로 리턴
    // LinkedList.class.getName() 으로 설정해주면 LinkedList 객체가 리턴됨
    props.put(CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_TYPE_CLASS, ArrayList.class.getName());

    // 밸류값의 경우엔 default.list.value.serde.inner
    props.put(
        CommonClientConfigs.DEFAULT_LIST_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.class.getName());

    // 밸류값의 경우엔 두 번째 인수가 false
    listDeserializer.configure(props, true);

    // 설정된 디시리얼라이저 얻어옴
    final Deserializer<?> inner = listDeserializer.innerDeserializer();
  }
}
