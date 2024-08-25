package com.assu.study.chap05;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

@RequiredArgsConstructor
public class TopicCreator {
  private final AdminClient adminClient;

  // 토픽 이름이 test 로 시작할 경우 토픽을 생성하는 메서드 (좋은 예시는 아님)
  public void maybeCreateTopic(String topicName) throws ExecutionException, InterruptedException {
    Collection<NewTopic> topics = new ArrayList<>();

    // 파티션 1개, replica 1개인 토픽 생성
    topics.add(new NewTopic(topicName, 1, (short) 1));

    if (topicName.toLowerCase().startsWith("test")) {
      adminClient.createTopics(topics);

      // 설정 변경
      ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
      ConfigEntry compaction =
          new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);

      Collection<AlterConfigOp> configOp = new ArrayList<>();
      configOp.add(new AlterConfigOp(compaction, AlterConfigOp.OpType.SET));

      Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = new HashMap<>();
      alterConfigs.put(configResource, configOp);

      adminClient.incrementalAlterConfigs(alterConfigs).all().get();
    }
  }
}
