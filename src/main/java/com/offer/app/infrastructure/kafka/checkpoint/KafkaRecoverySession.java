package com.offer.app.infrastructure.kafka.checkpoint;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

@Component
@Profile("postgres")
public class KafkaRecoverySession {

    private final KafkaRecoveryPositionResolver positionResolver;
    private final KafkaStateConsistencyCoordinator coordinator;

    private volatile boolean prepared;

    public KafkaRecoverySession(
            KafkaRecoveryPositionResolver positionResolver,
            KafkaStateConsistencyCoordinator coordinator
    ) {
        this.positionResolver =
                Objects.requireNonNull(positionResolver);
        this.coordinator =
                Objects.requireNonNull(coordinator);
    }

    public synchronized void prepare() {
        if (prepared) {
            throw new IllegalStateException(
                    "Recovery session already prepared"
            );
        }

        Map<TopicPartition, Long> positions =
                positionResolver.resolve();

        positions.forEach((tp, offset) ->
                coordinator.initializePartition(
                        tp.topic(),
                        tp.partition(),
                        offset
                )
        );

        prepared = true;
    }

    public void seekAssigned(
            Map<TopicPartition, Long> assignments,
            ConsumerSeekAware.ConsumerSeekCallback callback
    ) {
        requirePrepared();

        var offsets = coordinator.capture(
                () -> Boolean.TRUE
        ).nextOffsets();

        for (TopicPartition tp : assignments.keySet()) {
            Long nextOffset = offsets.get(tp);

            if (nextOffset == null) {
                throw new IllegalStateException(
                        "Missing recovery offset for " + tp
                );
            }

            callback.seek(
                    tp.topic(),
                    tp.partition(),
                    nextOffset
            );
        }
    }

    public void applyRecord(
            ConsumerRecord<?, ?> record,
            Runnable mutation
    ) {
        requirePrepared();

        coordinator.applyRecord(
                record.topic(),
                record.partition(),
                record.offset(),
                mutation
        );
    }

    private void requirePrepared() {
        if (!prepared) {
            throw new IllegalStateException(
                    "Kafka recovery session not prepared"
            );
        }
    }
}