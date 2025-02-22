package com.assu.study.chap14_1;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class WordCountExample {
  public static void main(String[] args) throws InterruptedException {
    Properties props = new Properties();

    // ===== 카프카 스트림즈 설정

    // 모든 카프카 스트림즈 애플리케이션은 애플리케이션 ID 를 가짐
    // 애플리케이션 ID 는 내부에서 사용하는 로컬 저장소와 여기 연관된 토픽에 이름을 정할때도 사용됨
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");

    // 카프카 스트림즈 애플리케이션은 항상 카프카 토픽에서 데이터를 읽어서 결과를 카프카 토픽에 씀
    // 카프카 스트림즈 애플리케이션은 인스턴스끼리 서로 협력하도록 할 때도 카프카를 사용하므로 애플리케이션이 카프카를 찾을 방법을 지정해주어야 함
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    // 데이터를 읽고 쓸 때 애플리케이션은 직렬화/역직렬화를 해야하므로 기본값으로 쓰일 Serde 클래스 지정
    // 필요하다면 스트림즈 토폴로지를 생성할 때 재정의할 수도 있음
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    // ===== 토폴로지 생성

    // StreamBuilder 객체를 생성하고, 앞으로 입력으로 사용할 토픽 지정
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, String> source = builder.stream("wordcount-input");

    final Pattern pattern = Pattern.compile("\\W+");
    KStream counts =
        source
            // 입력 토픽에서 읽어오는 각 이벤트는 단어들로 이루어진 문자열 한 줄임
            // 정규식을 사용하여 이 문자열을 다수의 단어들로 분할한 후 현재 이벤트 레코드의 밸류값인 각 단어를 가져다가 이벤트 레코드 키로 넣어줌으로써 그룹화에
            // 사용될 수 있도록 함
            .flatMapValues(
                value ->
                    Arrays.asList(pattern.split(value.toLowerCase()))) // KStream<String, String> 반환
            .map(
                (key, value) ->
                    new KeyValue<Object, Object>(value, value)) // KStream<Object, Object> 반환
            // 단어 "the" 를 필터링함 (필터링을 이렇게 쉽게 할 수 있음)
            .filter((key, value) -> (!value.equals("the"))) // KStream<Object, Object> 반환
            // 키 값을 기준으로 그룹화함으로써 각 단어별로 이벤트의 집합을 얻어냄
            .groupByKey() // KGroupedStream<Object, Object> 반환
            .count() // KTable<Object, Long> 반환
            // 각 집합에 얼마나 많은 이벤트가 포함되어 있는지 셈
            // 계산 결과는 Long 타입임
            .mapValues(value -> Long.toString(value))
            .toStream();

    // 결과를 카프카에 씀
    counts.to("wordcount-output");

    // ===== 애플리케이션 실행

    // 위에서 정의한 토폴로지와 설정값을 기준으로 KafkaStreams 객체 정의
    KafkaStreams streams = new KafkaStreams(builder.build(), props);

    // 동작을 재설정하기 위한 것임
    // 프로덕션에서는 절대 사용하지 말 것
    // 시작할 때마다 애플리케이션이 Kafka 의 상태를 재로드함
    streams.cleanUp();

    // 카프카 스트림즈 시작
    streams.start();

    Thread.sleep(5_000L);

    // 잠시 뒤 멈춤
    streams.close();
  }
}
