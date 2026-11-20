package com.offer.app.domain.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.offer.app.domain.model.status.EventMarketOutcomeStatus;

import java.math.BigDecimal;

public record EventMarketOutcome(
        String id,
        @JsonProperty("outcomeId")
        String marketOutcomeId,
        EventMarketOutcomeStatus status,
        BigDecimal odds
) {
    public EventMarketOutcome {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException(
                    "EventMarketOutcome ID is required"
            );
        }

        if (marketOutcomeId == null || marketOutcomeId.isBlank()) {
            throw new IllegalArgumentException(
                    "MarketOutcome ID is required"
            );
        }

        if (odds.signum() <= 0) {
            throw new IllegalArgumentException(
                    "EventMarketOutcome odds must be positive"
            );
        }
        EventMarketOutcomeStatus.requireNonNull(status);
    }
}
