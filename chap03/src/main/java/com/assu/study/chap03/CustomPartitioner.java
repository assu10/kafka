package com.assu.study.chap03;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

// 커스텀 파티셔너
public class CustomPartitioner implements Partitioner {

  // partition() 에 특정 데이터를 하드 코딩하는 것보다 configure() 를 경유하여 넘겨주는 게 좋지만 내용 이해를 위해 하드코딩
  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    int numPartitions = partitions.size();

    if ((keyBytes == null) || (!(key instanceof String))) {
      throw new InvalidRecordException("all messages to have customer name as key.");
    }

    if (((String) key).equals("A")) {
      // A 는 항상 마지막 파티션으로 지정
      return numPartitions - 1;
    }

    // 다른 키들은 남은 파티션에 해시값을 사용하여 할당
    return Math.abs(Utils.murmur2(keyBytes)) % (numPartitions - 1);
  }

  @Override
  public void close() {
  }


  @Override
  public void configure(Map<String, ?> map) {
  }
}
