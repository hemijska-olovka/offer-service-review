package com.offer.app.infrastructure.kafka.checkpoint;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KafkaAppliedOffsetTrackerTest {

    @Test
    void shouldTrackAppliedOffsets() {
        var tracker = new KafkaAppliedOffsetTracker();

        tracker.initialize("event", 0, 10);

        tracker.markApplied("event", 0, 10);
        tracker.markApplied("event", 0, 11);

        assertEquals(
                12L,
                tracker.snapshot().get(
                        new TopicPartition("event", 0)
                )
        );
    }

    @Test
    void shouldTrackPartitionsIndependently() {
        var tracker = new KafkaAppliedOffsetTracker();

        tracker.initialize("event", 0, 5);
        tracker.initialize("event", 1, 20);
        tracker.initialize("market", 0, 3);

        tracker.markApplied("event", 0, 5);
        tracker.markApplied("event", 1, 20);
        tracker.markApplied("market", 0, 3);

        var snapshot = tracker.snapshot();

        assertEquals(6L,
                snapshot.get(new TopicPartition("event", 0)));
        assertEquals(21L,
                snapshot.get(new TopicPartition("event", 1)));
        assertEquals(4L,
                snapshot.get(new TopicPartition("market", 0)));
    }

    @Test
    void shouldRejectSkippedOffsets() {
        var tracker = new KafkaAppliedOffsetTracker();

        tracker.initialize("event", 0, 10);

        assertThrows(
                IllegalStateException.class,
                () -> tracker.markApplied("event", 0, 12)
        );

        assertEquals(
                10L,
                tracker.snapshot().get(
                        new TopicPartition("event", 0)
                )
        );
    }

    @Test
    void shouldRejectUninitializedPartition() {
        var tracker = new KafkaAppliedOffsetTracker();

        assertThrows(
                IllegalStateException.class,
                () -> tracker.markApplied("event", 1, 10)
        );
    }
}
