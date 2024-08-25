package com.assu.study.chap05;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.errors.InvalidPartitionsException;

@Slf4j
public class AdminClientSample3 {
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
  }
}
