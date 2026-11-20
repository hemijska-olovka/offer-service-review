package com.offer.app.domain.model;

import com.offer.app.domain.model.status.EventMarketStatus;
import com.offer.app.domain.model.status.MarketStatus;

import java.util.List;

public record EventMarket(
        String id,
        String marketId,
        EventMarketStatus status,
        List<EventMarketOutcome> eventMarketOutcomes
) {
    public EventMarket {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException(
                    "Event Market ID is required"
            );
        }

        if (marketId == null || marketId.isBlank()) {
            throw new IllegalArgumentException(
                    "Market ID cannot be null or blank"
            );
        }

        EventMarketStatus.requireNonNull(status);
    }
}
