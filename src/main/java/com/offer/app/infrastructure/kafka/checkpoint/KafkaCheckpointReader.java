package com.offer.app.infrastructure.kafka.checkpoint;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Repository
@Profile("postgres")
public class KafkaCheckpointReader {

    private static final String SELECT_CHECKPOINT = """
            SELECT
                c.generation,
                o.partition_id,
                o.next_offset
            FROM offer_checkpoint c
            LEFT JOIN offer_checkpoint_offset o
                ON o.topic = c.topic
            WHERE c.topic = ?
            ORDER BY o.partition_id
            """;

    private final JdbcTemplate jdbcTemplate;

    public KafkaCheckpointReader(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate);
    }

    public Optional<TopicCheckpoint> findByTopic(String topic) {
        Objects.requireNonNull(topic);

        return jdbcTemplate.query(
                SELECT_CHECKPOINT,
                rs -> {
                    if (!rs.next()) {
                        return Optional.empty();
                    }

                    long generation = rs.getLong("generation");
                    Map<TopicPartition, Long> offsets = new HashMap<>();

                    do {
                        int partition = rs.getInt("partition_id");

                        if (!rs.wasNull()) {
                            long nextOffset = rs.getLong("next_offset");

                            offsets.put(
                                    new TopicPartition(topic, partition),
                                    nextOffset
                            );
                        }
                    } while (rs.next());

                    return Optional.of(
                            new TopicCheckpoint(generation, offsets)
                    );
                },
                topic
        );
    }

    public record TopicCheckpoint(
            long generation,
            Map<TopicPartition, Long> nextOffsets
    ) {
        public TopicCheckpoint {
            nextOffsets = Map.copyOf(nextOffsets);
        }
    }
}
