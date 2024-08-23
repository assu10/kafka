package com.assu.study.chap05;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

public class AdminClientSample {
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    AdminClient adminClient = AdminClient.create(props);

    // TODO: AdminClient 를 사용하여 필요한 작업 수행

    adminClient.close(Duration.ofSeconds(30));
  }
}
