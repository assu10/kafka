package com.assu.study.chap03_interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

// N밀리초마다 전송된 메시지 수와 확인된 메시지 수를 인쇄합니다.
public class CountingProducerInterceptor implements ProducerInterceptor {

  ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
  static AtomicLong numSent = new AtomicLong(0);
  static AtomicLong numAcked = new AtomicLong(0);

  @Override
  public void configure(Map<String, ?> map) {
    // ProducerInterceptor 는 Configurable 인터페이스를 구현하므로 configure() 메서드를 재정의함으로써 다른 메서드가
    // 호출되기 전에 뭔가 설정해주는 것이 가능함
    // 이 메서드는 전체 프로듀서 설정을 전달받기 때문에 어떠한 설정 매개변수도 읽거나 수정 가능
    Long windowSize = Long.valueOf((String) map.get("counting.interceptor.window.size.ms"));

    executorService.scheduleAtFixedRate(CountingProducerInterceptor::run, windowSize, windowSize, TimeUnit.MILLISECONDS);
  }

  // 프로듀서가 레코드를 브로커로 보내기 전, 직렬화되기 직전에 호출
  @Override
  public ProducerRecord onSend(ProducerRecord producerRecord) {
    numSent.incrementAndGet();

    // 레코드가 전송되면 전송된 레코드 수를 증가시키고, 레코드 자체는 변경하지 않은 채 그대로 리턴함
    return producerRecord;
  }

  // 브로커가 보낸 응답을 클라이언트가 받았을 때 호출
  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
    // 카프카카 ack 응답을 받으면 응답 수 변수를 증가시키고, 별도로 뭔가 리턴하지는 않음
    numAcked.incrementAndGet();
  }

  // 프로듀서에 close() 메서드가 호출될 때 호출됨
  // 인터셉터의 내부 상태를 정리하는 용도
  @Override
  public void close() {
    // 생성했던 스레드 풀 종료
    // 만일 파일을 열거나 원격 저장소에 연결을 생성했을 경우 여기서 닫아주어야 리소스 유실이 없음
    executorService.shutdownNow();
  }


  public static void run() {
    System.out.println(numSent.getAndSet(0));
    System.out.println(numAcked.getAndSet(0));
  }
}
