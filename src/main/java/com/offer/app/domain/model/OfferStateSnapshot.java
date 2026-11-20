package com.offer.app.domain.model;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public record OfferStateSnapshot(
        List<Event> events,
        List<Market> markets
) {

    public OfferStateSnapshot {
        events = List.copyOf(
                Objects.requireNonNull(events, "Events are required")
        );

        markets = List.copyOf(
                Objects.requireNonNull(markets, "Markets are required")
        );

        Set<String> eventIds = new HashSet<>();

        for (Event event : events) {
            if (!eventIds.add(event.id())) {
                throw new IllegalArgumentException(
                        "Duplicate Event ID: " + event.id()
                );
            }
        }

        Set<String> marketIds = new HashSet<>();

        for (Market market : markets) {
            if (!marketIds.add(market.id())) {
                throw new IllegalArgumentException(
                        "Duplicate Market ID: " + market.id()
                );
            }
        }
    }
}
