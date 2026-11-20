package com.offer.app.domain.model;

import com.offer.app.domain.model.status.MarketStatus;

import java.util.List;

public record Market(
        String id,
        String name,
        MarketStatus status,
        List<MarketOutcome> marketOutcomes
) {
    public Market {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException(
                    "Market ID is required"
            );
        }

        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException(
                    "Market name is required"
            );
        }

        MarketStatus.requireNonNull(status);
    }
}
