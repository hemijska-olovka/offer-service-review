package com.offer.app.infrastructure.kafka.checkpoint;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;

import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Profile("postgres")
public class KafkaRecoveryPositionResolver {

    private static final Map<String, Set<Integer>> PARTITIONS =
            Map.of(
                    "market", Set.of(0),
                    "event", Set.of(0, 1)
            );

    private final KafkaCheckpointReader checkpointReader;
    private final JdbcTemplate jdbcTemplate;
    private final KafkaAdmin kafkaAdmin;

    public KafkaRecoveryPositionResolver(
            KafkaCheckpointReader checkpointReader,
            JdbcTemplate jdbcTemplate,
            KafkaAdmin kafkaAdmin
    ) {
        this.checkpointReader =
                Objects.requireNonNull(checkpointReader);
        this.jdbcTemplate =
                Objects.requireNonNull(jdbcTemplate);
        this.kafkaAdmin =
                Objects.requireNonNull(kafkaAdmin);
    }

    public Map<TopicPartition, Long> resolve() {

        Map<TopicPartition, OffsetSpec> earliestRequests =
                new HashMap<>();

        Map<TopicPartition, OffsetSpec> latestRequests =
                new HashMap<>();

        PARTITIONS.forEach((topic, partitions) -> {
            for (int partition : partitions) {
                var tp = new TopicPartition(topic, partition);

                earliestRequests.put(tp, OffsetSpec.earliest());
                latestRequests.put(tp, OffsetSpec.latest());
            }
        });

        try (Admin admin = Admin.create(
                kafkaAdmin.getConfigurationProperties()
        )) {

            var earliest = admin.listOffsets(earliestRequests)
                    .all()
                    .get(10, TimeUnit.SECONDS);

            var latest = admin.listOffsets(latestRequests)
                    .all()
                    .get(10, TimeUnit.SECONDS);

            Map<TopicPartition, Long> result =
                    new HashMap<>();

            for (var entry : PARTITIONS.entrySet()) {

                String topic = entry.getKey();

                Set<TopicPartition> expected = entry.getValue()
                        .stream()
                        .map(p -> new TopicPartition(topic, p))
                        .collect(java.util.stream.Collectors.toSet());

                var checkpoint = checkpointReader.findByTopic(topic);

                if (checkpoint.isEmpty()
                        && countStoredAggregates(topic) != 0) {
                    throw new IllegalStateException(
                            "Stored state exists without checkpoint: "
                                    + topic
                    );
                }

                if (checkpoint.isPresent()
                        && !checkpoint.get().nextOffsets()
                        .keySet().equals(expected)) {
                    throw new IllegalStateException(
                            "Incomplete checkpoint for " + topic
                    );
                }

                for (TopicPartition tp : expected) {

                    long nextOffset = checkpoint
                            .map(c -> c.nextOffsets().get(tp))
                            .orElse(0L);

                    long beginning = earliest.get(tp).offset();
                    long end = latest.get(tp).offset();

                    if (nextOffset < beginning
                            || nextOffset > end) {
                        throw new IllegalStateException(
                                "Kafka recovery offset unavailable: "
                                        + tp
                                        + ", requested=" + nextOffset
                                        + ", earliest=" + beginning
                                        + ", end=" + end
                        );
                    }

                    result.put(tp, nextOffset);
                }
            }

            return Map.copyOf(result);

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();

            throw new IllegalStateException(
                    "Kafka recovery interrupted", ex
            );

        } catch (ExecutionException | TimeoutException ex) {
            throw new IllegalStateException(
                    "Cannot resolve Kafka recovery positions", ex
            );
        }
    }

    private long countStoredAggregates(String topic) {

        String table = switch (topic) {
            case "market" -> "offer_market_state";
            case "event" -> "offer_event_state";
            default -> throw new IllegalArgumentException(
                    "Unsupported topic: " + topic
            );
        };

        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + table,
                Long.class
        );
    }
}
