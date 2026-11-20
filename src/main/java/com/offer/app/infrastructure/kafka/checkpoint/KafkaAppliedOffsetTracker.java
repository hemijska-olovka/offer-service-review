package com.offer.app.infrastructure.kafka.checkpoint;

import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Component
public final class KafkaAppliedOffsetTracker {

    private final Map<TopicPartition, Long> nextOffsets =
            new HashMap<>();

    public synchronized void initialize(
            String topic,
            int partition,
            long nextOffset
    ) {
        Objects.requireNonNull(topic);

        if (partition < 0 || nextOffset < 0) {
            throw new IllegalArgumentException(
                    "Partition and offset must be non-negative"
            );
        }

        TopicPartition key = new TopicPartition(
                topic,
                partition
        );

        if (nextOffsets.containsKey(key)) {
            throw new IllegalStateException(
                    "Partition already initialized: " + key
            );
        }

        nextOffsets.put(key, nextOffset);
    }

    public synchronized void markApplied(
            String topic,
            int partition,
            long processedOffset
    ) {
        TopicPartition key = new TopicPartition(
                topic,
                partition
        );

        Long expectedOffset = nextOffsets.get(key);

        if (expectedOffset == null) {
            throw new IllegalStateException(
                    "Partition not initialized: " + key
            );
        }

        if (processedOffset != expectedOffset) {
            throw new IllegalStateException(
                    "Unexpected Kafka offset for " + key
                            + ". Expected: " + expectedOffset
                            + ", received: " + processedOffset
            );
        }

        nextOffsets.put(
                key,
                Math.addExact(processedOffset, 1)
        );
    }

    public synchronized Map<TopicPartition, Long> snapshot() {
        return Map.copyOf(nextOffsets);
    }
}