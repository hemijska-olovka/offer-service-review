package com.offer.app.infrastructure.persistence;

import com.offer.app.domain.model.Event;
import com.offer.app.domain.model.Market;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;
import java.util.Objects;

@Repository
@Profile("postgres")
public class OfferStatePersistenceImpl implements OfferStatePersistence {
    private static final String UPSERT_MARKET = """
            INSERT INTO offer_market_state (
                id,
                payload,
                persisted_at
            )
            VALUES (?, ?::jsonb, CURRENT_TIMESTAMP)
            ON CONFLICT (id)
            DO UPDATE SET
                payload = EXCLUDED.payload,
                persisted_at = CURRENT_TIMESTAMP
            WHERE offer_market_state.payload
                  IS DISTINCT FROM EXCLUDED.payload
            """;

    private static final String UPSERT_EVENT = """
            INSERT INTO offer_event_state (
                id,
                payload,
                persisted_at
            )
            VALUES (?, ?::jsonb, CURRENT_TIMESTAMP)
            ON CONFLICT (id)
            DO UPDATE SET
                payload = EXCLUDED.payload,
                persisted_at = CURRENT_TIMESTAMP
            WHERE offer_event_state.payload
                  IS DISTINCT FROM EXCLUDED.payload
            """;
    private final JdbcTemplate jdbcTemplate;
    private final JsonMapper jsonMapper;

    public OfferStatePersistenceImpl(JdbcTemplate jdbcTemplate, JsonMapper jsonMapper) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate);
        this.jsonMapper = Objects.requireNonNull(jsonMapper);
    }

    @Override
    public void upsertMarket(Market market) {
        Objects.requireNonNull(market);

        String payload = jsonMapper.writeValueAsString(market);

        jdbcTemplate.update(
                UPSERT_MARKET,
                market.id(),
                payload
        );
    }

    @Override
    public void upsertEvent(Event event) {
        Objects.requireNonNull(event);

        String payload = jsonMapper.writeValueAsString(event);

        jdbcTemplate.update(
                UPSERT_EVENT,
                event.id(),
                payload
        );
    }
}
