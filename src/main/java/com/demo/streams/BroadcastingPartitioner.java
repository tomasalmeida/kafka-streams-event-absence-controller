package com.demo.streams;

import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BroadcastingPartitioner<K, V> implements StreamPartitioner<K, V> {

    @Override
    public Integer partition(String topic, K key, V value, int numPartitions) {
        return null;
    }

    @Override
    public Optional<Set<Integer>> partitions(String topic, K key, V value, int numPartitions) {
        return Optional.of(IntStream.range(0, numPartitions)
                .boxed()
                .collect(Collectors.toSet()));
    }
}
