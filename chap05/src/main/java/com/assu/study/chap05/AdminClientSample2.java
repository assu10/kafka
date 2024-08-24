package com.assu.study.chap05;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Slf4j
public class AdminClientSample2 {
  // private static final String TOPIC_NAME = "sample-topic";
  private static final String CONSUMER_GROUP = "TestConsumerGroup";
  private static final List<String> CONSUMER_GROUP_LIST = List.of(CONSUMER_GROUP);

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000); // optional
    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 1000); // optional
    AdminClient adminClient = AdminClient.create(props);

    // ======= 컨슈머 그룹 조회
    adminClient.listConsumerGroups().valid().get().forEach(System.out::println);

    // ======= 특정 컨슈머 그룹의 상세 정보 조회
    ConsumerGroupDescription groupDescription =
        adminClient
            .describeConsumerGroups(CONSUMER_GROUP_LIST) // DescribeConsumerGroupsResult 반환
            .describedGroups() // Map<String, KafkaFuture<ConsumerGroupDescription>> 반환
            .get(CONSUMER_GROUP) // KafkaFuture<ConsumerGroupDescription> 반환
            .get();

    log.info("Description of Consumer group: {} - {}", CONSUMER_GROUP, groupDescription);

    // ======= 컨슈머 그룹에서 오프셋 커밋 정보 조회
    // 1)
    // 컨슈머 그룹이 사용 중인 모든 토픽 파티션을 key 로, 각각의 토픽 파티션에 대해 마지막으로 커밋된 오프셋을 value 로 하는 맵 조회
    // `describeConsumerGroups()` 와 달리 `listConsumerGroupOffsets()` 은 컨슈머 그룹의 모음이 아닌 하나의 컨슈머 그룹을 받음
    Map<TopicPartition, OffsetAndMetadata> offsets =
        adminClient.listConsumerGroupOffsets(CONSUMER_GROUP).partitionsToOffsetAndMetadata().get();

    Map<TopicPartition, OffsetSpec> reqLatestOffsets = new HashMap<>();
    Map<TopicPartition, OffsetSpec> reqEarliestOffsets = new HashMap<>();
    Map<TopicPartition, OffsetSpec> reqOlderOffsets = new HashMap<>();

    Instant resetTo = ZonedDateTime.now(ZoneId.of("Asia/Seoul")).minusHours(2).toInstant();

    // 2)
    // 컨슈머 그룹에서 커밋한 토픽의 모든 파티션에 대해 최신 오프셋, 가장 오래된 오프셋, 2시간 전의 오프셋 조회
    for (TopicPartition tp : offsets.keySet()) {
      reqLatestOffsets.put(tp, OffsetSpec.latest());
      reqEarliestOffsets.put(tp, OffsetSpec.earliest());
      reqOlderOffsets.put(tp, OffsetSpec.forTimestamp(resetTo.toEpochMilli()));
    }

    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
        adminClient.listOffsets(reqLatestOffsets).all().get();
    //    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets =
    //        adminClient.listOffsets(reqEarliestOffsets).all().get();
    //    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> orderOffsets =
    //        adminClient.listOffsets(reqOlderOffsets).all().get();

    // 3)
    // 모든 파티션을 반복해서 각각의 파티션에 대해 마지막으로 커밋된 오프셋, 파티션의 마지막 오프셋, 둘 사이의 lag 출력
    for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
      String topic = e.getKey().topic();
      int partition = e.getKey().partition();
      long committedOffset = e.getValue().offset();

      long latestOffset = latestOffsets.get(e.getKey()).offset();
      // 아래는 확실하지 않음
      //      long earliestOffset = earliestOffsets.get(e.getKey()).offset();
      //      long orderOffset = orderOffsets.get(e.getKey()).offset();

      log.info(
          "Consumer group {} has committed offset {} to topic {}, partition {}.",
          CONSUMER_GROUP,
          committedOffset,
          topic,
          partition);
      log.info(
          "The latest offset in the partition is {} to consumer group is {} records behind.",
          latestOffset,
          (latestOffset - committedOffset));
      // 아래는 확실하지 않음
      // log.info("The earliest offset: {}, order offset: {}", earliestOffset, orderOffset);
    }
  }
}
