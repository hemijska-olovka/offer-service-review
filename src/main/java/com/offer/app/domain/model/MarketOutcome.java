package com.offer.app.domain.model;

import com.offer.app.domain.model.status.MarketOutcomeStatus;

public record MarketOutcome(
        String id,
        String name,
        MarketOutcomeStatus status
) {
    public MarketOutcome {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException(
                    "MarketOutcome ID is required"
            );
        }

        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException(
                    "MarketOutcome name is required"
            );
        }

        MarketOutcomeStatus.requireNonNull(status);
    }
}
