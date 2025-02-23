package com.assu.study.chap14_3;

import com.assu.study.chap14_3.model.PageView;
import com.assu.study.chap14_3.model.Search;
import com.assu.study.chap14_3.model.UserActivity;
import com.assu.study.chap14_3.model.UserProfile;
import com.assu.study.chap14_3.serde.CustomJsonDeserializer;
import com.assu.study.chap14_3.serde.CustomJsonSerializer;
import com.assu.study.chap14_3.serde.CustomWrapperSerde;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;

public class Chap143Application {

  public static void main(String[] args) throws InterruptedException {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "clicks");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER);

    // 스트림의 각 단계는 서로 다른 객체가 포함되기 때문에 기본 Serde 사용 불가

    // 미리 로드된 동일한 데이터를 데모 코드로 재실행하기 위해 오프셋을 earliest 로 설정
    // 데모를 재실행하려면 오프셋 재설정 도구를 사용해야 함
    // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    StreamsBuilder builder = new StreamsBuilder();

    // ===== 다수의 스트림을 조인하는 토폴로지

    // 조인하고자 하는 2개의 스트림 객체인 클릭과 검색 스트림 생성
    // 스트림 객체 생성 시 입력 토픽 뿐 아니라 토픽 데이터를 읽어서 객체로 역직렬화할 때 사용될 키, 밸류에 대한 Serde 역시 지정해주어야 함
    KStream<Integer, PageView> views =
        builder.stream(
            Constants.PAGE_VIEW_TOPIC, Consumed.with(Serdes.Integer(), new PageViewSerde()));

    KStream<Integer, Search> searches =
        builder.stream(Constants.SEARCH_TOPIC, Consumed.with(Serdes.Integer(), new SearchSerde()));

    // 사용자 프로필을 저장할 KTable 객체 정의
    // KTable 은 변경 스트림에 의해 갱신되는 구체화된 저장소(materialized store)임
    KTable<Integer, UserProfile> profiles =
        builder.table(
            Constants.USER_PROFILE_TOPIC, Consumed.with(Serdes.Integer(), new ProfileSerde()));

    // 스트림-테이블 조인
    KStream<Integer, UserActivity> viewsWithProfile =
        // 클릭 스트림을 사용자 프로필 정보 테이블과 조인함으로써 확장함
        // 스트림-테이블 조인에서 스트림의 각 이벤트는 프로필 테이블의 캐시된 사본에서 정보를 받음
        // left join 이므로 해당하는 사용자 정보가 없는 클릭도 보존됨
        views.leftJoin(
            profiles,
            (page, profile) -> {
              if (profile != null) {
                // 조인 메서드임
                // 스트림과 레코드에서 하나씩 값을 받아서 또 다른 값을 리턴함
                // 두 개의 값을 결합해서 어떻게 하나의 결과로 만들지 여기서 결정해야 함
                // 여기서는 사용자 프로필과 페이지 뷰 둘 다 포함하는 하나의 UserActivity 객체를 생성함
                return new UserActivity(
                    profile.getUserId(),
                    profile.getUserName(),
                    profile.getZipcode(),
                    profile.getInterests(),
                    "",
                    page.getPage());
              } else {
                return new UserActivity(-1, "", "", null, "", page.getPage());
              }
            });

    // 스트림-스트림 조인
    KStream<Integer, UserActivity> userActivityKStream =
        // 같은 사용자에 의해 수행된 클릭 정보와 검색 정보 조인
        // 이번엔 스트림을 테이블에 조인하는 것이 아니라 두 개의 스트림을 조인하는 것임
        viewsWithProfile.leftJoin(
            searches,
            (userActivity, search) -> {
              if (search != null) {
                // 조인 메서드임
                // 단순히 맞춰지는 모든 페이지 뷰에 검색어들을 추가해줌
                userActivity.updateSearch(search.getSearchTerms());
              } else {
                userActivity.updateSearch("");
              }
              return userActivity;
            },
            // 스트림-스트림 조인은 시간 윈도우를 사용하는 조인이므로 각 사용자의 모든 클릭과 검색을 조인하는 것은 적절하지 않음
            // 검색 이후 짧은 시간 안에 발생한 클릭을 조인함으로써 검색과 거기 연관된 클릭만을 조인해야 함
            // 따라서 1초 길이의 조인 윈도우를 정의(= 검색 전과 후의 1초 길이의 윈도우)한 뒤 0초 간격으로 before() 를 호출해서 검색 후 1초 동안
            // 발생한 클릭만 조인함
            // 이 때 검색 전 1초는 제외됨
            // 그 결과는 관련이 있는 클릭과 검색어, 사용자 프로필을 포함하게 됨
            // 즉, 검색과 그 결과 전체에 대해 분석 수행이 가능해짐
            JoinWindows.of(Duration.ofSeconds(1)).before(Duration.ofSeconds(0)),
            // 조인 결과에 대한 Serde 를 정의함
            // 조인 양쪽에 공통인 키 값에 대한 Serde 와 조인 결과에 포함될 양쪽의 밸류값에 대한 Serde 를 포함함
            // 여기서는 키는 사용자 id 이므로 단순한 Integer 형 Serde 를 사용함
            StreamJoined.with(Serdes.Integer(), new UserActivitySerde(), new SearchSerde()));

    userActivityKStream.to(
        Constants.USER_ACTIVITY_TOPIC, Produced.with(Serdes.Integer(), new UserActivitySerde()));

    KafkaStreams streams = new KafkaStreams(builder.build(), props);

    // 동작을 재설정하기 위한 것임
    // 프로덕션에서는 절대 사용하지 말 것
    // 시작할 때마다 애플리케이션이 Kafka 의 상태를 재로드함
    streams.cleanUp();

    streams.start();

    Thread.sleep(60_000L);

    // 잠시 뒤 멈춤
    streams.close();
  }

  public static final class PageViewSerde extends CustomWrapperSerde<PageView> {
    public PageViewSerde() {
      super(new CustomJsonSerializer<>(), new CustomJsonDeserializer<>(PageView.class));
    }
  }

  public static final class ProfileSerde extends CustomWrapperSerde<UserProfile> {
    public ProfileSerde() {
      super(new CustomJsonSerializer<>(), new CustomJsonDeserializer<>(UserProfile.class));
    }
  }

  public static final class SearchSerde extends CustomWrapperSerde<Search> {
    public SearchSerde() {
      super(new CustomJsonSerializer<>(), new CustomJsonDeserializer<>(Search.class));
    }
  }

  public static final class UserActivitySerde extends CustomWrapperSerde<UserActivity> {
    public UserActivitySerde() {
      super(new CustomJsonSerializer<>(), new CustomJsonDeserializer<>(UserActivity.class));
    }
  }
}
