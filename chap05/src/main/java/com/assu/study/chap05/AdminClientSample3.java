package com.assu.study.chap05;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.apache.kafka.common.errors.InvalidPartitionsException;

@Slf4j
public class AdminClientSample3 {
  public static final String CONSUMER_GROUP = "testConsumerGroup";
  public static final String TOPIC_NAME = "sample-topic";
  public static final int NUM_PARTITIONS = 6;

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000); // optional
    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 1000); // optional
    AdminClient adminClient = AdminClient.create(props);

    // ======= 토픽에 파티션 추가
    Map<String, NewPartitions> newPartitions = new HashMap<>();

    // 토픽의 파티션을 확장할 때는 새로 추가될 파티션 수가 아닌 파티션이 추가된 뒤의 파티션 수를 지정해야 함
    newPartitions.put(TOPIC_NAME, NewPartitions.increaseTo(NUM_PARTITIONS + 2));

    try {
      adminClient.createPartitions(newPartitions).all().get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof InvalidPartitionsException) {
        log.error("Couldn't modify number of partitions in topic: {}", e.getMessage());
      }
    }

    // ======= 토픽에서 레코드 삭제
    Map<TopicPartition, OffsetAndMetadata> offsets =
        adminClient.listConsumerGroupOffsets(CONSUMER_GROUP).partitionsToOffsetAndMetadata().get();

    Map<TopicPartition, OffsetSpec> reqOrderOffsets = new HashMap<>();
    Instant resetTo = ZonedDateTime.now(ZoneId.of("Asia/Seoul")).minusHours(2).toInstant();

    for (TopicPartition tp : offsets.keySet()) {
      reqOrderOffsets.put(tp, OffsetSpec.forTimestamp(resetTo.toEpochMilli()));
    }

    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> olderOffsets =
        adminClient.listOffsets(reqOrderOffsets).all().get();

    Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
    for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e :
        olderOffsets.entrySet()) {
      recordsToDelete.put(e.getKey(), RecordsToDelete.beforeOffset(e.getValue().offset()));
    }

    adminClient.deleteRecords(recordsToDelete).all().get();

    // ======= 리더 선출
    Set<TopicPartition> electableTopics = new HashSet<>();
    electableTopics.add(new TopicPartition(TOPIC_NAME, 0));

    try {
      // 1)
      // 특정 토픽의 한 파티션에 대해 선호 리더 선출
      // 지정할 수 있는 토픽과 파티션 수에는 제한이 업음
      // 만일 파티션 모음이 아닌 null 값을 지정하여 아래 명령어를 실행할 경우 모든 파티션에 대해 지정된 선출 유형 작업을 시작함
      adminClient.electLeaders(ElectionType.PREFERRED, electableTopics).all().get();
    } catch (ExecutionException e) {
      // 2)
      // 클러스터 상태가 좋다면 아무런 작업도 일어나지 않을 것임
      // 선호 리더 선출과 언클린 리더 선출은 선호 리더가 아닌 replica 가 현재 리더를 맡고 있을 경우에만 수행됨
      if (e.getCause() instanceof ElectionNotNeededException) {
        log.error("All leaders are preferred leaders, no need to do anything.");
      }
    }
  }
}
