package com.offer.app.infrastructure.kafka.checkpoint;

import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

@Component
public final class KafkaStateConsistencyCoordinator {

    private final KafkaAppliedOffsetTracker offsetTracker;

    private final ReentrantReadWriteLock lock =
            new ReentrantReadWriteLock();

    public KafkaStateConsistencyCoordinator(
            KafkaAppliedOffsetTracker offsetTracker
    ) {
        this.offsetTracker = Objects.requireNonNull(offsetTracker);
    }

    public void initializePartition(
            String topic,
            int partition,
            long nextOffset
    ) {
        lock.writeLock().lock();

        try {
            offsetTracker.initialize(
                    topic,
                    partition,
                    nextOffset
            );
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void applyRecord(
            String topic,
            int partition,
            long offset,
            Runnable mutation
    ) {
        Objects.requireNonNull(mutation);

        lock.writeLock().lock();

        try {
            TopicPartition key =
                    new TopicPartition(topic, partition);

            Long expectedOffset =
                    offsetTracker.snapshot().get(key);

            if (expectedOffset == null ||
                    expectedOffset != offset) {
                throw new IllegalStateException(
                        "Unexpected Kafka offset for " + key
                                + ". Expected: " + expectedOffset
                                + ", received: " + offset
                );
            }

            // Update in-memory state.
            mutation.run();

            // Advance offset only after successful mutation.
            offsetTracker.markApplied(
                    topic,
                    partition,
                    offset
            );

        } finally {
            lock.writeLock().unlock();
        }
    }

    public <T> ConsistentSnapshot<T> capture(
            Supplier<T> stateSupplier
    ) {
        Objects.requireNonNull(stateSupplier);

        lock.readLock().lock();

        try {
            return new ConsistentSnapshot<>(
                    stateSupplier.get(),
                    offsetTracker.snapshot()
            );
        } finally {
            lock.readLock().unlock();
        }
    }

    public record ConsistentSnapshot<T>(
            T state,
            Map<TopicPartition, Long> nextOffsets
    ) {
    }
}
