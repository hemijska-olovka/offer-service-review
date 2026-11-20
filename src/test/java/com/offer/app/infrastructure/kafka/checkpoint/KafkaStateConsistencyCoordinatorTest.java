package com.offer.app.infrastructure.kafka.checkpoint;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class KafkaStateConsistencyCoordinatorTest {

    @Test
    void shouldApplyMutationAndAdvanceOffset() {
        var coordinator = new KafkaStateConsistencyCoordinator(
                new KafkaAppliedOffsetTracker()
        );

        coordinator.initializePartition("event", 0, 10);

        AtomicInteger mutations = new AtomicInteger();

        coordinator.applyRecord(
                "event", 0, 10,
                mutations::incrementAndGet
        );

        var snapshot = coordinator.capture(
                mutations::get
        );

        assertEquals(1, snapshot.state());

        assertEquals(
                11L,
                snapshot.nextOffsets().get(
                        new TopicPartition("event", 0)
                )
        );
    }

    @Test
    void shouldNotAdvanceOffsetWhenMutationFails() {
        var coordinator = new KafkaStateConsistencyCoordinator(
                new KafkaAppliedOffsetTracker()
        );

        coordinator.initializePartition("event", 0, 10);

        assertThrows(
                IllegalStateException.class,
                () -> coordinator.applyRecord(
                        "event", 0, 10,
                        () -> {
                            throw new IllegalStateException(
                                    "Mutation failed"
                            );
                        }
                )
        );

        var snapshot = coordinator.capture(() -> "unchanged");

        assertEquals(
                10L,
                snapshot.nextOffsets().get(
                        new TopicPartition("event", 0)
                )
        );
    }

    @Test
    void shouldRejectOffsetBeforeRunningMutation() {
        var coordinator = new KafkaStateConsistencyCoordinator(
                new KafkaAppliedOffsetTracker()
        );

        coordinator.initializePartition("market", 0, 5);

        AtomicInteger mutations = new AtomicInteger();

        assertThrows(
                IllegalStateException.class,
                () -> coordinator.applyRecord(
                        "market", 0, 7,
                        mutations::incrementAndGet
                )
        );

        assertEquals(0, mutations.get());
    }
}
