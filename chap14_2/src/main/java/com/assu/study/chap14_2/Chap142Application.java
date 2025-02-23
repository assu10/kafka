package com.assu.study.chap14_2;

import com.assu.study.chap14_2.model.Trade;
import com.assu.study.chap14_2.model.TradeStats;
import com.assu.study.chap14_2.serde.CustomJsonDeserializer;
import com.assu.study.chap14_2.serde.CustomJsonSerializer;
import com.assu.study.chap14_2.serde.CustomWrapperSerde;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;

// input 은 거래의 스트림임
// output 은 2개의 스트림임
// - 10초마다 최소 및 평균 매도 호가를 가짐
// - 매분 최소 매도 호가가 가장 낮은 상위 3개 주식
public class Chap142Application {

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    // ===== 애플리케이션 설정
    Properties props;
    if (args.length == 1) {
      props = LoadConfig.loadConfig(args[0]);
    } else {
      props = LoadConfig.loadConfig();
    }
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stockstat-2");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TradeSerde.class.getName());

    // 미리 로드된 동일한 데이터를 데모 코드로 재실행하기 위해 오프셋을 earliest 로 설정
    // 데모를 재실행하려면 오프셋 재설정 도구를 사용해야 함
    // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // 집계 윈도우의 시간 간격(ms)
    long windowSize = 5000; // (5s)

    // AdminClient 를 생성하고 클러스터의 브로커 수를 확인하여 원하는 replicas 수를 알 수 있음
    AdminClient adminClient = AdminClient.create(props);
    DescribeClusterResult describeClusterResult = adminClient.describeCluster();
    int clusterSize = describeClusterResult.nodes().get().size();

    if (clusterSize < 3) {
      props.put("replication.factor", clusterSize);
    } else {
      props.put("replication.factor", 3);
    }

    // ===== 스트림즈 토폴로지 생성
    // StreamBuilder 객체를 생성하고, 앞으로 입력으로 사용할 토픽 지정
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String, Trade> source = builder.stream(Constants.STOCK_TOPIC);

    // 토폴로지 생성
    KStream<Windowed<String>, TradeStats> stats =
        source
            // 입력 토픽에서 이벤트를 읽어오지만 그룹화는 하지 않음
            // 대신 이벤트 스트림이 레코드 키 기준으로 파티셔닝되도록 해줌
            // 이 경우 토픽에 데이터를 쓸 때의 키 값을 가지는 데이터를 사용하고, groupByKey() 를 호출하기 전에 변경하지 않았으므로
            // 데이터는 여전히 키 값을 기준으로 파티셔닝되어 있음
            .groupByKey() // KGroupedStream<String, Trade>
            // 윈도우 정의
            // 이 경우 윈도우는 5초의 길이를 갖고 있으며, 매초 전진함
            .windowedBy(
                TimeWindows.of(Duration.ofMillis(windowSize))
                    .advanceBy(Duration.ofSeconds(1))) // TimeWindowedKStream<Stream<String, Trade>>
            // 데이터가 원하는대로 파티셔닝되고 윈도우도 적용되었으면 집계 작업을 시작함
            // aggregate() 는 스트림을 서로 중첩되는 윈도우들도 나눈 뒤(여기서는 1초마다 겹치는 5초 길기의 시간 윈도우)
            // 각 윈도우에 배정된 모든 이벤트에 대해 집계 연산을 적용함
            .<TradeStats>aggregate(
                // 첫 번째 파라메터는 집계 결과를 저장할 새로운 객체를 받음, 여기서는 TradeStats
                // 이 객체는 각 시간 윈도우에서 알고자하는 모든 통계를 포함하기 위해 생성한 객체로 최저 매도가, 평균 매도가, 거래량을 포함함
                () -> new TradeStats(),
                // 실제로 집계를 수행하는 메서드 지정
                // 여기서는 새로운 레코드를 생성함으로써 해당 윈도우에서의 최저 매도가, 거래량, 매도 총량을 업데이트하기 위해 TradeStats.add() 를
                // 사용함
                (k, v, tradestats) -> tradestats.add(v),
                Materialized
                    // 윈도우가 적용된 집계 작업에서는 상태를 저장할 로컬 저장소를 유지할 필요가 있음
                    // aggregate() 의 마지막 파라메터는 상태 저장소 설정임
                    // Materialized 는 저장소를 설정하는데 사용되는 객체로서 여기서는 저장소의 이름을 trade-aggregates 로 함
                    .<String, TradeStats, WindowStore<Bytes, byte[]>>as("trade-aggregates")
                    // 상태 저장소 설정의 일부로서 집계 결과인 Tradestats 를 직렬화/역직렬화하기 위한 Serde 객체를 지정해주어야 함
                    .withValueSerde(new TradeStatsSerde())) // KTable<Windowed<String>, TradeStats>
            // 집계 결과는 종목 기호와 시간 윈도우를 기본키로, 집계 결과를 밸류값으로 하는 테이블임
            // 이 테이블을 이벤트 스트림으로 되돌릴것임
            .toStream() // KStream<Windowed<String>, TradeStats>
            // 평균 가격을 갱신해 줌
            // 현재 시점에서 집계 결과는 가격과 거래량의 합계를 포함함
            // 이 레코드를 사용하여 평균 가격을 계산한 뒤 출력 스트림으로 내보냄
            .mapValues((trade) -> trade.computeAvgPrice());

    // 결과를 stockstats-output 스트림에 씀
    // 결과물이 윈도우 작업의 일부이므로 결과물을 윈도우 타임스탬프와 함께 윈도우가 적용된 데이터 형식으로 저장하는 WindowedSerde 를 생성해줌
    // 윈도우 크기가 직렬화 과정에서 사용되는 것은 아니지만 출력 토픽에 윈도우의 시작 시간이 저장되기 때문에 역직렬화는 윈도우의 크기를 필요로하므로 Serde 의 일부로서
    // 전달함
    stats.to(
        "stockstats-output",
        Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize)));

    // ===== 스트림 객체 생성 후 실행
    Topology topology = builder.build();
    KafkaStreams streams = new KafkaStreams(topology, props);
    System.out.println(topology.describe());
    // 동작을 재설정하기 위한 것임
    // 프로덕션에서는 절대 사용하지 말 것
    // 시작할 때마다 애플리케이션이 Kafka 의 상태를 재로드함
    streams.cleanUp();

    streams.start();

    // SIGTERM 에 응답하고 카프카 스트림즈를 gracefully 하게 종료시키는 shutdown hook 추가
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  // 키는 문자열이지만 밸류는 종목 코드, 매도 호가, 매도량을 포함하는 Trade 를 사용할 것이므로 이 객체를
  // 직렬화/역직렬화하기 위해 Gson 라이브러리를 사용해서 자바 객체에 대한 시리얼라이저/디시리얼라이저 생성
  public static final class TradeSerde extends CustomWrapperSerde<Trade> {
    public TradeSerde() {
      super(new CustomJsonSerializer<Trade>(), new CustomJsonDeserializer<>(Trade.class));
    }
  }

  public static final class TradeStatsSerde extends CustomWrapperSerde<TradeStats> {
    public TradeStatsSerde() {
      super(new CustomJsonSerializer<>(), new CustomJsonDeserializer<>());
    }
  }
}
