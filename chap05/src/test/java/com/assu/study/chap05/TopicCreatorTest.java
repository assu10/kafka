package com.assu.study.chap05;

import static org.mockito.Mockito.*;

import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

// mock 을 사용하여 TopicCreator 테스트
class TopicCreatorTest {
  private AdminClient adminClient;

  @BeforeEach
  void setUp() {
    // id 가 0 인 브로커 생성
    Node broker = new Node(0, "localhost", 9092);

    // 브로커 목록과 컨트롤러를 지정하여 MockAdminClient 객체 생성 (여기서는 하나만 지정함)
    // 나중에 TopicCreator 가 제대로 실행되었는지 확인하기 위해 spy() 메서드의 주입 기능을 사용할 것임
    this.adminClient = spy(new MockAdminClient(List.of(broker), broker));

    // 아래 내용이 없으면 테스트 시
    // `java.lang.UnsupportedOperationException: Not implemented yet` 예외 발생됨

    // Mockito 의 doReturn() 메서드를 사용하여 mock-up 클라이언트가 예외를 발생시키지 않도록 함
    // 테스트하고자 하는 메서드는 AlterConfigResult 객체가 필요하고,
    // 이 객체는 KafkaFuture 객체를 리턴하는 all() 메서드가 있어야 함
    // 가짜 incrementalAlterConfigs() 메서드는 정확히 이것을 리턴해야 함
    //    AlterConfigsResult emptyResult = mock(AlterConfigsResult.class);
    //    doReturn(KafkaFuture.completedFuture(null)).when(emptyResult).all();
    //    doReturn(emptyResult).when(adminClient).incrementalAlterConfigs(any());
  }

  @Test
  public void testCreateTopic() throws ExecutionException, InterruptedException {
    TopicCreator tc = new TopicCreator(adminClient);
    tc.maybeCreateTopic("test.is.a.test.topic");

    // createTopics() 가 한 번 호출되었는지 확인
    verify(adminClient, times(1)).createTopics(any());
  }

  @Test
  public void tetNotTopic() throws ExecutionException, InterruptedException {
    TopicCreator tc = new TopicCreator(adminClient);
    tc.maybeCreateTopic("not.a.test");

    // 토픽 이름이 test 로 시작하지 않을 경우 createTopics() 가 한 번도 호출되지 않았는지 확인
    verify(adminClient, never()).createTopics(any());
  }
}
