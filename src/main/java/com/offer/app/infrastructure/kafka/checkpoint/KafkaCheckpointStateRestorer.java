package com.offer.app.infrastructure.kafka.checkpoint;
import com.offer.app.application.repository.OfferStateRepository;
import com.offer.app.domain.model.Event;
import com.offer.app.domain.model.Market;
import com.offer.app.domain.model.OfferStateSnapshot;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Objects;

@Component
@Profile("postgres")
public class KafkaCheckpointStateRestorer {

    private final KafkaCheckpointReader checkpointReader;
    private final OfferStateRepository stateRepository;
    private final JdbcTemplate jdbcTemplate;
    private final JsonMapper jsonMapper;

    public KafkaCheckpointStateRestorer(
            KafkaCheckpointReader checkpointReader,
            OfferStateRepository stateRepository,
            JdbcTemplate jdbcTemplate,
            JsonMapper jsonMapper
    ) {
        this.checkpointReader =
                Objects.requireNonNull(checkpointReader);
        this.stateRepository =
                Objects.requireNonNull(stateRepository);
        this.jdbcTemplate =
                Objects.requireNonNull(jdbcTemplate);
        this.jsonMapper =
                Objects.requireNonNull(jsonMapper);
    }

    public void restore() {

        // Initial JSON state is already loaded.
        var initialState = stateRepository.loadSnapshot();

        List<Market> markets =
                checkpointReader.findByTopic("market").isPresent()
                        ? loadMarkets()
                        : initialState.markets();

        List<Event> events =
                checkpointReader.findByTopic("event").isPresent()
                        ? loadEvents()
                        : initialState.events();

        stateRepository.replaceSnapshot(
                new OfferStateSnapshot(events, markets)
        );
    }

    private List<Market> loadMarkets() {
        return jdbcTemplate.query(
                """
                SELECT payload::text
                FROM offer_market_state
                ORDER BY id
                """,
                (rs, rowNum) -> jsonMapper.readValue(
                        rs.getString(1),
                        Market.class
                )
        );
    }

    private List<Event> loadEvents() {
        return jdbcTemplate.query(
                """
                SELECT payload::text
                FROM offer_event_state
                ORDER BY id
                """,
                (rs, rowNum) -> jsonMapper.readValue(
                        rs.getString(1),
                        Event.class
                )
        );
    }
}