package com.offer.app.infrastructure.notification;

import com.offer.app.domain.model.Event;
import com.offer.app.domain.model.Market;

import com.offer.app.infrastructure.persistence.OfferStatePersistence;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Repository
@Profile("postgres")
public class KafkaCheckpointWriter {

    private static final Map<String, Integer> PARTITION_COUNTS =
            Map.of(
                    "market", 1,
                    "event", 2
            );

    private static final String ADVANCE_CHECKPOINT = """
            INSERT INTO offer_checkpoint (
                topic, generation, completed_at
            )
            VALUES (?, 1, CURRENT_TIMESTAMP)
            ON CONFLICT (topic)
            DO UPDATE SET
                generation = offer_checkpoint.generation + 1,
                completed_at = CURRENT_TIMESTAMP
            RETURNING generation
            """;

    private static final String READ_OFFSETS = """
            SELECT partition_id, next_offset
            FROM offer_checkpoint_offset
            WHERE topic = ?
            """;

    private static final String INSERT_OFFSET = """
            INSERT INTO offer_checkpoint_offset (
                topic, partition_id, next_offset
            )
            VALUES (?, ?, ?)
            """;

    private final JdbcTemplate jdbcTemplate;
    private final OfferStatePersistence persistence;
    private final TransactionTemplate transactionTemplate;

    public KafkaCheckpointWriter(
            JdbcTemplate jdbcTemplate,
            OfferStatePersistence persistence,
            PlatformTransactionManager transactionManager
    ) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate);
        this.persistence = Objects.requireNonNull(persistence);

        this.transactionTemplate =
                new TransactionTemplate(transactionManager);

        this.transactionTemplate.setPropagationBehavior(
                TransactionDefinition.PROPAGATION_REQUIRES_NEW
        );
    }

    public long writeCompleteMarketSnapshot(
            List<Market> markets,
            Map<TopicPartition, Long> nextOffsets
    ) {
        List<Market> snapshot = List.copyOf(markets);

        List<String> ids = snapshot.stream()
                .map(Market::id)
                .toList();

        validateIds(ids);

        return writeCheckpoint(
                "market",
                ids,
                nextOffsets,
                () -> snapshot.forEach(persistence::upsertMarket)
        );
    }

    public long writeCompleteEventSnapshot(
            List<Event> events,
            Map<TopicPartition, Long> nextOffsets
    ) {
        List<Event> snapshot = List.copyOf(events);

        List<String> ids = snapshot.stream()
                .map(Event::id)
                .toList();

        validateIds(ids);

        return writeCheckpoint(
                "event",
                ids,
                nextOffsets,
                () -> snapshot.forEach(persistence::upsertEvent)
        );
    }

    private long writeCheckpoint(
            String topic,
            List<String> ids,
            Map<TopicPartition, Long> allOffsets,
            Runnable persistAggregates
    ) {
        Map<Integer, Long> offsets =
                validateAndSelectOffsets(topic, allOffsets);

        Long generation = transactionTemplate.execute(status -> {

            // Locks the checkpoint row until transaction commit.
            Long nextGeneration = jdbcTemplate.queryForObject(
                    ADVANCE_CHECKPOINT,
                    Long.class,
                    topic
            );

            Objects.requireNonNull(nextGeneration);

            Map<Integer, Long> previousOffsets =
                    readPreviousOffsets(topic);

            validateProgress(
                    topic,
                    nextGeneration,
                    previousOffsets,
                    offsets
            );

            // Same JDBC transaction.
            persistAggregates.run();

            // Full snapshot semantics, including deletions.
            deleteMissingAggregates(topic, ids);

            jdbcTemplate.update(
                    "DELETE FROM offer_checkpoint_offset WHERE topic = ?",
                    topic
            );

            for (int partition = 0;
                 partition < PARTITION_COUNTS.get(topic);
                 partition++) {

                jdbcTemplate.update(
                        INSERT_OFFSET,
                        topic,
                        partition,
                        offsets.get(partition)
                );
            }

            return nextGeneration;
        });

        return Objects.requireNonNull(generation);
    }

    private Map<Integer, Long> readPreviousOffsets(String topic) {
        return jdbcTemplate.query(
                READ_OFFSETS,
                rs -> {
                    Map<Integer, Long> result = new HashMap<>();

                    while (rs.next()) {
                        result.put(
                                rs.getInt("partition_id"),
                                rs.getLong("next_offset")
                        );
                    }

                    return result;
                },
                topic
        );
    }

    private Map<Integer, Long> validateAndSelectOffsets(
            String topic,
            Map<TopicPartition, Long> allOffsets
    ) {
        Objects.requireNonNull(allOffsets);

        int expectedCount = PARTITION_COUNTS.get(topic);

        Map<Integer, Long> result = new HashMap<>();

        for (var entry : allOffsets.entrySet()) {
            TopicPartition key = entry.getKey();

            if (!topic.equals(key.topic())) {
                continue;
            }

            int partition = key.partition();
            long offset = entry.getValue();

            if (partition < 0 ||
                    partition >= expectedCount ||
                    offset < 0) {
                throw new IllegalArgumentException(
                        "Invalid checkpoint position: " + key
                );
            }

            result.put(partition, offset);
        }

        if (result.size() != expectedCount) {
            throw new IllegalStateException(
                    "Incomplete checkpoint offsets for " + topic
            );
        }

        return Map.copyOf(result);
    }

    private void validateProgress(
            String topic,
            long generation,
            Map<Integer, Long> previous,
            Map<Integer, Long> current
    ) {
        if (generation == 1) {
            if (!previous.isEmpty()) {
                throw new IllegalStateException(
                        "Unexpected existing offsets for " + topic
                );
            }
            return;
        }

        if (!previous.keySet().equals(current.keySet())) {
            throw new IllegalStateException(
                    "Existing checkpoint is incomplete for " + topic
            );
        }

        boolean advanced = false;

        for (int partition : current.keySet()) {
            long oldOffset = previous.get(partition);
            long newOffset = current.get(partition);

            if (newOffset < oldOffset) {
                throw new IllegalStateException(
                        "Checkpoint regression: " + topic
                                + "-" + partition
                );
            }

            if (newOffset > oldOffset) {
                advanced = true;
            }
        }

        if (!advanced) {
            throw new IllegalStateException(
                    "Checkpoint has not advanced for " + topic
            );
        }
    }

    private void deleteMissingAggregates(
            String topic,
            List<String> ids
    ) {
        String table = switch (topic) {
            case "market" -> "offer_market_state";
            case "event" -> "offer_event_state";
            default -> throw new IllegalArgumentException(
                    "Unsupported topic: " + topic
            );
        };

        String sql = "DELETE FROM " + table
                + " WHERE id <> ALL (?::text[])";

        jdbcTemplate.update(connection -> {
            var statement = connection.prepareStatement(sql);

            var array = connection.createArrayOf(
                    "text",
                    ids.toArray(String[]::new)
            );

            statement.setArray(1, array);

            return statement;
        });
    }

    private static void validateIds(List<String> ids) {
        Set<String> uniqueIds = new HashSet<>();

        for (String id : ids) {
            if (id == null || id.isBlank() ||
                    !uniqueIds.add(id)) {
                throw new IllegalArgumentException(
                        "Invalid or duplicate aggregate id: " + id
                );
            }
        }
    }
}
