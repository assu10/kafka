package com.assu.study.chap05;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerOptions;
import java.util.List;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

// Test with: curl 'localhost:8080?topic=demo-topic'
// 실제로 비동기인지 확인하려면 SIGSTOP 을 사용하여 카프카를 중지시킨 후 아래 실행
// curl 'localhost:8080?topic=demo-topic&timeout=60000' on one terminal
// curl 'localhost:8080?topic=demo-topic' on another

// Vertx 스레드가 하나만 있어도 첫 번째 명령은 60초를 기다리고, 두 번째 명령은 즉시 반환됨
// 두 번째 명령이 첫 번째 명령 이후에 블록되지 않았음을 알 수 있음
@Slf4j
public class AdminRestServer {
  private static Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(1));

  public static void main(String[] args) {
    // AdminClient 초기화
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000); // request.timeout.ms
    props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 1000); // default.api.timeout.ms
    AdminClient adminClient = AdminClient.create(props);

    // 서버 생성
    HttpServerOptions options = new HttpServerOptions().setLogActivity(true);
    vertx
        .createHttpServer(options)
        // Vertx 를 사용하여 간단한 HTTP 서버 생성
        // 이 서버는 요청을 받을 때마다 requestHandler 호출함
        .requestHandler( // 1)
            req -> {
              // 요청은 매개 변수로 토픽 이름을 보내고, 응답은 토픽의 상세 설정을 내보냄
              String topic = req.getParam("topic"); // 2)
              String timeout = req.getParam("timeout");
              int timeoutMs = NumberUtils.toInt(timeout, 1000); // timeout 이 없으면 디폴트 1000ms

              // AdminClient.describeTopics() 를 호출하여 응답에 들어있는 Future 객체를 받아옴
              DescribeTopicsResult demoTopic =
                  adminClient.describeTopics( // 3)
                      List.of(topic), new DescribeTopicsOptions().timeoutMs(timeoutMs));

              demoTopic
                  .topicNameValues() // Map<String, KafkaFuture<TopicDescription>> 반환
                  .get(topic) // KafkaFuture<TopicDescription> 반환
                  // 호출 시 블록되는 get() 을 호출하는 대신 Future 객체의 작업이 완료되면 호출될 함수 생성
                  .whenComplete( // 4)
                      (topicDescription, throwable) -> {
                        if (throwable != null) {
                          log.info("got exception");

                          // Future 가 예외를 발생시키면서 완료될 경우 HTTP 클라이언트에 에러를 보냄
                          req.response()
                              .end(
                                  "Error trying to describe topic "
                                      + topic
                                      + " due to "
                                      + throwable.getMessage()); // 5)
                        } else {
                          // Future 가 성공적으로 완료될 경우 HTTP 클라이언트에 토픽의 상세 정보를 보냄
                          req.response().end(topicDescription.toString()); // 6)
                        }
                      });
            })
        .listen(8080);
  }
}
