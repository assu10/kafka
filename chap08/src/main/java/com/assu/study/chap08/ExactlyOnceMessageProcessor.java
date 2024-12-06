package com.assu.study.chap08;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;

/** 읽기-처리-쓰기 애플리케이션 */
public class ExactlyOnceMessageProcessor extends Thread {
  private final String transactionalId;
  private final KafkaProducer<Integer, String> producer;
  private final KafkaConsumer<Integer, String> consumer;
  private final String inputTopic;
  private final String outTopic;

  public ExactlyOnceMessageProcessor(String threadName, String inputTopic, String outTopic) {
    this.transactionalId = "tid-" + threadName;
    this.inputTopic = inputTopic;
    this.outTopic = outTopic;

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer"); // client.id

    // 프로듀서에 transactional.id 를 설정해줌으로써 다수의 파티션에 대해 원자적 쓰기가 가능한 트랜잭션적 프로듀서 생성
    // 트랜잭션 ID 는 고유하고, 오랫동안 유지되어야 함
    producerProps.put(
        ProducerConfig.TRANSACTIONAL_ID_CONFIG, this.transactionalId); // transactional.id

    producer = new KafkaProducer<>(producerProps);

    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoGroupId"); // group.id

    // 트랜잭션의 일부가 되는 컨슈머는 오프셋을 직접 커밋하지 않으며, 프로듀서가 트랜잭션 과정의 일부로써 오프셋을 커밋함
    // 따라서 오프셋 커밋 기능은 꺼야 함
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // enable.auto.commit

    // 이 예시에서 컨슈머는 입력 토픽을 읽어옴
    // 여기서는 입력 토픽의 레코드들 역시 트랜잭션적 프로듀서에 의해 쓰였다고 가정함
    // 진행중이거나 실패한 트랙잭션들은 무시하기 위해 컨슈머 격리 수준을 read_committed 로 설정
    // 컨슈머는 커밋된 트랜잭션 외에도 비 트랜잭션적 쓰기 역시 읽어온다는 것을 유념할 것
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed"); // isolation.level

    consumer = new KafkaConsumer<>(consumerProps);
  }

  @Override
  public void run() {
    // 트랜잭션적 프로듀서 사용 시 가장 먼저 할 일은 초기화 작업
    // 이것은 트랜잭션 ID 를 등록하고, 동일한 트랜잭션 ID 를 갖는 다른 프로듀서들이 좀비로 인식될 수 있도록 에포크 값을 증가시킴
    // 같은 트랜잭션 ID 를 사용하는, 아직 진행중인 트랜잭션들 역시 중단됨
    producer.initTransactions();

    // subscribe() 컨슈머 API 사용
    // 이 애플리케이션 인스턴스에 할당된 파티션들은 언제든지 리밸런스의 결과로서 변경될 수 있음
    // subscribe() 는 트랜잭션에 컨슈머 그룹 정보를 추가하고, 이를 사용하여 좀비 펜싱을 수행함
    // subscribe() 를 사용할 때는 관련된 파티션이 할당 해제될 때마다 트랜잭션을 커밋해주는 것이 좋음
    consumer.subscribe(Collections.singleton(inputTopic));

    while (true) {
      try {
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(200));
        if (records.count() > 0) {
          // 레코드를 읽어왔으니 처리하여 결과를 생성함
          // beginTransaction() 은 호출된 시점부터 트랜잭션이 종료(커밋/중단)되는 사이 쓰여진 모든 레코드들이 하나의 원자적 트랜잭션의 일부임을 보장함
          producer.beginTransaction();

          for (ConsumerRecord<Integer, String> record : records) {
            // 레코드를 처리해주는 곳
            // 모든 비즈니스 로직이 여기 들어감
            ProducerRecord<Integer, String> customizedRecord = transform(record);
            producer.send(customizedRecord);
          }

          Map<TopicPartition, OffsetAndMetadata> offsets = getOffsetsToCommit(consumer);

          // 트랜잭션 작업 도중에 오프셋을 커밋해주는 것이 중요함
          // 결과 쓰기에 실패하더라도 처리되지 않은 레코드 오프셋이 커밋되지 않도록 보장해줌
          // 다른 방식으로 오프셋을 커밋하면 안된다는 점을 유의해야 함
          // 자동 커밋 기능은 끄고, 컨슈머의 커밋 API 도 호출하지 않아야 함
          // 다른 방식으로 오프셋을 커밋할 경우 트랜잭션 보장이 적용되지 않음
          producer.sendOffsetsToTransaction(getOffsetsToCommit(consumer), consumer.groupMetadata());

          // 필요한 메시지를 모두 쓰고 오프셋을 커밋했으니 이제 트랜잭션을 커밋하고 작업을 마무리함
          // 이 메서드가 성공적으로 리턴하면 전체 트랜잭션이 완료된 것임
          // 이제 다음 이벤트 배치를 읽어와서 처리해주는 작업을 계속할 수 있음
          producer.commitTransaction();
        }
      } catch (ProducerFencedException | InvalidProducerEpochException e) {
        // 이 예외가 발생한다면 현재의 프로세서가 좀비가 된 것임
        // 애플리케이션이 처리를 멈췄든 연결이 끊어졌든 같은 트랜잭션 ID 를 갖는 애플리케이션의 새로운 인스턴스가 실행되고 있을 것임
        // 여기서 시작한 트랜잭션은 이미 중단되었고, 어디선가 그 레코드들을 대신 처리하고 있을 가능성이 높음
        // 이럴 땐 이 애플리케이션을 종료하는 것 외엔 할 수 있는 조치가 없음
        throw new KafkaException(
            String.format("The transactional.id %s is used by another process", transactionalId));
      } catch (KafkaException e) {
        // 트랜잭션을 쓰는 도중에 엥러가 발생한 경우 트랜잭션을 중단시키고 컨슈머의 오프셋 위치를 뒤로 돌린 후 재시도함
        producer.abortTransaction();
        resetToLastCommittedPositions(consumer);
      }
    }
  }

  // 레코드를 처리해주는 곳
  // 모든 비즈니스 로직이 여기 들어감
  private ProducerRecord<Integer, String> transform(ConsumerRecord<Integer, String> record) {
    return new ProducerRecord<>(outTopic, record.key(), record.value() + "-ok");
  }

  private Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit(
      KafkaConsumer<Integer, String> consumer) {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    for (TopicPartition topicPartition : consumer.assignment()) {
      offsets.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition), null));
    }
    return offsets;
  }

  private void resetToLastCommittedPositions(KafkaConsumer<Integer, String> consumer) {
    Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(consumer.assignment());
    consumer
        .assignment()
        .forEach(
            tp -> {
              OffsetAndMetadata offsetAndMetadata = committed.get(tp);
              if (offsetAndMetadata != null) {
                consumer.seek(tp, offsetAndMetadata.offset());
              } else {
                consumer.seekToBeginning(Collections.singleton(tp));
              }
            });
  }
}
