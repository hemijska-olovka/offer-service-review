package com.offer.app.infrastructure.kafka.checkpoint;

import com.offer.app.application.repository.OfferStateRepository;
import com.offer.app.infrastructure.notification.KafkaCheckpointWriter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Objects;
import org.apache.kafka.common.TopicPartition;
import java.util.Map;
import java.util.HashMap;
@Component
@Profile("postgres")
public class KafkaCheckpointSnapshotService {

    private final KafkaStateConsistencyCoordinator coordinator;
    private final ObjectProvider<OfferStateRepository> stateRepository;
    private final KafkaCheckpointWriter checkpointWriter;

    public KafkaCheckpointSnapshotService(
            KafkaStateConsistencyCoordinator coordinator,
            ObjectProvider<OfferStateRepository> stateRepositoryObjectProvider,
            KafkaCheckpointWriter checkpointWriter
    ) {
        this.coordinator = Objects.requireNonNull(coordinator);
        this.stateRepository = Objects.requireNonNull(stateRepositoryObjectProvider);
        this.checkpointWriter = Objects.requireNonNull(checkpointWriter);
    }

    public void persistMarketCheckpoint() {
        var snapshot = coordinator.capture(
                () -> stateRepository.getObject().loadSnapshot()
        );

        checkpointWriter.writeCompleteMarketSnapshot(
                snapshot.state().markets(),
                offsetsForTopic(snapshot.nextOffsets(), "market")
        );
    }

    public void persistEventCheckpoint() {
        var snapshot = coordinator.capture(
                () -> stateRepository.getObject().loadSnapshot()
        );

        checkpointWriter.writeCompleteEventSnapshot(
                snapshot.state().events(),
                offsetsForTopic(snapshot.nextOffsets(), "event")
        );
    }

    private Map<TopicPartition, Long> offsetsForTopic(
            Map<TopicPartition, Long> offsets,
            String topic
    ) {
        Map<TopicPartition, Long> result = new HashMap<>();

        offsets.forEach((partition, nextOffset) -> {
            if (partition.topic().equals(topic)) {
                result.put(partition, nextOffset);
            }
        });

        if (result.isEmpty()) {
            throw new IllegalStateException(
                    "No initialized Kafka offsets for topic: " + topic
            );
        }

        return Map.copyOf(result);
    }
}
