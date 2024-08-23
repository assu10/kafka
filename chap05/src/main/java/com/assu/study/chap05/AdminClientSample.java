package com.assu.study.chap05;

import java.util.*;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

@Slf4j
public class AdminClientSample {
  private static final String TOPIC_NAME = "sample-topic";
  private static final List<String> TOPIC_LIST = List.of(TOPIC_NAME);
  private static final int NUM_PARTITIONS = 6;
  private static final short REPLICATION_FACTOR = 1;

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000); // optional
    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 1000); // optional
    AdminClient adminClient = AdminClient.create(props);

    TopicDescription topicDescription;

    // ======= 클러스터에 있는 토픽 목록 조회

    ListTopicsResult topics = adminClient.listTopics(); // Future 객체들을 ListTopicsResult 객체 리턴
    topics.names().get().forEach(System.out::println);

    // adminClient.close(Duration.ofSeconds(30));

    // ======= 특정 토픽이 있는지 확인 후 없으면 토픽 생성

    // 정확한 설정을 갖는 토픽이 존재하는지 확인하려면 확인하려는 토픽의 목록을 인자로 넣어서 describeTopics() 메서드 호출
    // 리턴되는 DescribeTopicsResult 객체 안에는 토픽 이름을 key 로, 토픽에 대한 상세 정보를 담는 Future 객체를 value 로 하는 맵이 들어있음
    DescribeTopicsResult sampleTopic = adminClient.describeTopics(TOPIC_LIST); // 1)
    try {
      // Future 가 완료될 때까지 기다린다면 get() 을 사용하여 원하는 결과물 (여기선 TopicDescription) 을 얻을 수 있음
      // 하지만 서버가 요청을 제대로 처리하면 못할 수도 있음
      // (만일 토픽이 존재하지 않으면 서버가 상세 정보를 보내줄 수도 없음)
      // 이 경우 서버는 에러를 리턴할 것이고, Future 는 ExecutionException 을 발생시킴
      // 예외의 cause 에 들어있는 것이 서버가 실제 리턴한 실제 에러임
      // 여기선 토픽이 존재하지 않을 경우를 처리하고 싶은 것 (= 토픽이 존재하지 않으면 토픽 생성) 이므로 이 예외를 처리해주어야 함
      topicDescription = sampleTopic.topicNameValues().get(TOPIC_NAME).get(); // 2)
      log.info("Description of sample topic: {}", topicDescription);

      // 토픽이 존재할 경우 Future 객체는 토픽에 속한 모든 파티션의 목록을 담은 TopicDescription 을 리턴함
      // TopicDescription 는 파티션별로 어느 브로커가 리더이고, 어디에 replica 가 있고, in-sync replica 가 무엇인지까지 포함함
      // 주의할 점은 토픽의 설정은 포함되지 않는다는 점임
      // 토픽 설정에 대해선 추후 다룰 예정
      if (topicDescription.partitions().size() != NUM_PARTITIONS) { // 3)
        log.error("Topic has wrong number of partitions: {}", topicDescription.partitions().size());
        // System.exit(1);
      }
    } catch (ExecutionException e) { // 4) 토픽이 존재하지 않은 경우에 대한 처리
      // 모든 예외에 대해 바로 종료
      if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
        log.error(e.getMessage());
        throw e;
      }

      // 여기까지 오면 토픽이 존재하지 않는 것임
      log.info("Topic {} does not exist. Going to create it now.", TOPIC_NAME);

      // 토픽이 존재하지 않을 경우 새로운 토픽 생성
      // 토픽 생성 시 토픽의 이름만 필수이고, 파티션 수와 replica 수는 선택사항임
      // 만일 이 값들을 지정하지 않으면 카프카 브로커에 설정된 기본값이 사용됨
      CreateTopicsResult newTopic =
          adminClient.createTopics(
              List.of(new NewTopic(TOPIC_NAME, NUM_PARTITIONS, REPLICATION_FACTOR))); // 5)

      // 잘못된 수의 파티션으로 토픽으로 생성되었는지 확인하려면 아래 주석 해제
      //      CreateTopicsResult newWrongTopic =
      //          adminClient.createTopics(
      //              List.of(new NewTopic(TOPIC_NAME, Optional.empty(), Optional.empty())));

      // 토픽이 정상적으로 생성되었는지 확인
      // 여기서는 파티션의 수를 확인하고 있음
      // CreateTopic 의 결과물을 확인하기 위해 get() 을 다시 호출하고 있기 때문에 이 메서드가 예외를 발생시킬 수 있음
      // 이 경우 TopicExistsException 이 발생하는 것이 보통이며, 이것을 처리해 주어야 함
      // 보통은 설정을 확인하기 위해 토픽 상세 내역을 조회함으로써 처리함
      if (newTopic.numPartitions(TOPIC_NAME).get() != NUM_PARTITIONS) { // 6)
        log.error("Topic was created with wrong number of partitions. Exiting.");
        System.exit(1);
      }
    }

    // ======= 토픽 압착(compacted) 설정 확인 및 교정

    // 특정한 토픽의 설정 확인
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME); // 1)
    DescribeConfigsResult configsResult = adminClient.describeConfigs(List.of(configResource));
    Config configs = configsResult.all().get().get(configResource);

    // 기본값이 아닌 설정 출력
    // describeConfig() 의 결과물인 DescribeConfigsResult 는 ConfigResource 를 key 로, 설정값의 모음을 value 로 갖는
    // 맵임
    // 각 설정 항목은 해당 값이 기본값에서 변경되었는지 확인할 수 있게 해주는 isDefault() 메서드를 가짐
    // 토픽 설정이 기본값이 아닌 것으로 취급되는 경우는 2 가지 경우가 있음
    // 1. 사용자가 토픽의 설정값을 기본값이 아닌 것으로 잡아준 경우
    // 2. 브로커 단위 설정이 수정된 상태에서 토픽이 생성되어 기본값이 아닌 값을 브로커 설정으로부터 상속받은 경우
    configs.entries().stream()
        .filter(entry -> !entry.isDefault())
        .forEach(System.out::println); // 2)

    // 토픽에 압착 (compacted) 설정이 되어있는지 확인
    ConfigEntry compaction =
        new ConfigEntry(
            TopicConfig.CLEANUP_POLICY_CONFIG,
            TopicConfig.CLEANUP_POLICY_COMPACT); // cleanup.policy, compact

    // 토픽에 압착 설정이 되어있지 않을 경우 압착 설정해 줌
    if (!configs.entries().contains(compaction)) {
      Collection<AlterConfigOp> configOp = new ArrayList<>();

      // 설정을 변경하려면 변경하고자 하는 ConfigResource 를 key 로, 바꾸고자 하는 설정값 모음을 value 로 하는 맵을 지정함
      // 각각의 설정 변경 작업은 설정 항목 (= 설정의 이름과 설정값. 여기서는 cleanup.policy 가 설정 이름이고, compact 가 설정값) 과 작업 유형으로
      // 이루어짐
      // 카프카에서는 4가지 형태의 설정 변경이 가능함
      // 설정값을 잡아주는 SET / 현재 설정값을 삭제하고 기본값으로 되돌리는 DELETE / APPEND / SUBSTRACT
      // APPEND 와 SUBSTRACT 는 목록 형태의 설정에만 사용 가능하며, 이걸 사용하면 전체 목록을 주고받을 필요없이 필요한 설정만 추가/삭제 가능함
      configOp.add(new AlterConfigOp(compaction, AlterConfigOp.OpType.SET)); // 3)
      Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = new HashMap<>();
      alterConfigs.put(configResource, configOp);
      adminClient.incrementalAlterConfigs(alterConfigs).all().get();
    } else {
      log.info("Topic {} is compacted topic.", TOPIC_NAME);
    }

    // ======= 토픽 삭제
    adminClient.deleteTopics(TOPIC_LIST).all().get();

    try {
      // 토픽이 삭제되었는지 확인
      // 삭제 작업이 비동기적으로 이루어지므로 이 시점에서 토픽이 여전히 남아있을 수 있음
      sampleTopic.topicNameValues().get(TOPIC_NAME).get();
      log.info("Topic {} is still around.");
    } catch (ExecutionException e) {
      log.info("Topic {} is gone.", TOPIC_NAME);
    }
  }
}
