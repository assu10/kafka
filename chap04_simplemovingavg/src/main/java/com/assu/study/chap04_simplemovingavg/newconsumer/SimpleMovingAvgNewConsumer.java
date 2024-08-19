package com.assu.study.chap04_simplemovingavg.newconsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

// 메인 애플리케이션 스레드에서 돌아가는 컨슈머의 실행 루프 종료
@Slf4j
public class SimpleMovingAvgNewConsumer {
  private Properties kafkaProps = new Properties();
  private String waitTime;
  private KafkaConsumer<String, String> consumer;

  public static void main(String[] args) {
    if (args.length == 0) {
      log.error("need args: {brokers} {group.id} {topic} {window-size}");
      return;
    }

    final SimpleMovingAvgNewConsumer movingAvg = new SimpleMovingAvgNewConsumer();
    String brokers = args[0];
    String groupId = args[1];
    String topic = args[2];
    int windowSize = Integer.parseInt(args[3]);

    CircularFifoQueue queue = new CircularFifoQueue(windowSize);
    movingAvg.configure(brokers, groupId);

    final Thread mainThread = Thread.currentThread();

    // ShutdownHook 은 별개의 스레드에서 실행되므로 폴링 루프를 탈출하기 위해 할 수 있는 것은 wakeup() 을 호출하는 것 뿐임
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              @Override
              public void run() {
                log.info("Starting exit...");
                movingAvg.consumer.wakeup();

                try {
                  mainThread.join();
                } catch (InterruptedException e) {
                  log.error("error: {}", e.getMessage());
                }
              }
            });

    try {
      movingAvg.consumer.subscribe(Collections.singletonList(topic));

      Duration timeout = Duration.ofMillis(10000); // 10초

      //  Ctrl+c 가 눌리면 shutdownhook 정리 및 종료
      while (true) {
        // 타임아웃을 매우 길게 설정함
        // 만일 폴링 루프가 충분히 짧아서 종료되기 전에 좀 기다리는게 별로 문제가 되지 않는다면 굳이 wakeup() 을 호출해줄 필요가 없음
        // 즉, 그냥 이터레이션마다 아토믹 boolean 값을 확인하는 것만으로 충분함
        // 폴링 타임아웃을 길게 잡아주는 이유는 메시지가 조금씩 쌓이는 토픽에서 데이터를 읽어올 때 편리하기 때문임
        // 이 방법을 사용하면 브로커가 리턴할 새로운 데이터를 가지고 있지 않은 동안 계속해서 루프를 돌면서도 더 적은 CPU 를 사용할 수 있음
        ConsumerRecords<String, String> records = movingAvg.consumer.poll(timeout);
        log.info("{} -- waiting for data...", System.currentTimeMillis());

        for (ConsumerRecord<String, String> record : records) {
          log.info(
              "topic: {}, partition: {}, offset: {}, key: {}, value: {}",
              record.topic(),
              record.partition(),
              record.offset(),
              record.key(),
              record.value());

          int sum = 0;

          try {
            int num = Integer.parseInt(record.value());
            queue.add(num);
          } catch (NumberFormatException e) {
            // just ignore strings
          }

          for (Object o : queue) {
            sum += (Integer) o;
          }

          if (queue.size() > 0) {
            log.info("Moving avg is {}", (sum / queue.size()));
          }
        }
        for (TopicPartition tp : movingAvg.consumer.assignment()) {
          log.info("Committing offset at position: {}", movingAvg.consumer.position(tp));
        }

        movingAvg.consumer.commitSync();
      }
    } catch (WakeupException e) {
      // ignore for shutdown
      // 다른 스레드에서 wakeup() 을 호출할 경우, 폴링 루프에서 WakeupException 발생
      // 발생된 예외를 잡아줌으로써 애플리케이션이 예기치않게 종료되지 않도록 할 수 있지만 딱히 뭔가를 추가적으로 해 줄 필요ㅇ는 없음
    } finally {
      // 컨슈머 종료 전 닫아서 리소스 정리
      movingAvg.consumer.close();
      log.info("Closed consumer and done.");
    }
  }

  private void configure(String servers, String groupId) {
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, servers);
    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
    // 유효한 오프셋이 없을 경우 파티션의 맨 처음부터 모든 데이터 읽음
    kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    consumer = new KafkaConsumer<>(kafkaProps);
  }
}
