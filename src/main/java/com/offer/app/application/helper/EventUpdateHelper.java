package com.offer.app.application.helper;

import com.offer.app.domain.model.Event;
import com.offer.app.domain.model.EventMarket;
import com.offer.app.domain.model.status.EventStatus;

import java.time.LocalDateTime;
import java.util.*;


public record EventUpdateHelper(
        String id,
        Optional<String> name,
        Optional<LocalDateTime> startsAt,
        Optional<EventStatus> status,
        Optional<List<EventMarket>> markets
) {

    public EventUpdateHelper {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException(
                    "Event ID cannot be null or blank"
            );
        }

        name = Objects.requireNonNull(
                name, "Name field cannot be null"
        );

        startsAt = Objects.requireNonNull(
                startsAt, "StartsAt field cannot be null"
        );

        status = Objects.requireNonNull(
                status, "Status field cannot be null"
        );

        markets = Objects.requireNonNull(
                markets, "Markets field cannot be null"
        );

        name.ifPresent(value -> {
            if (value.isBlank()) {
                throw new IllegalArgumentException(
                        "Event name cannot be blank"
                );
            }
        });

        markets = markets.map(EventUpdateHelper::validateMarkets);
    }

    private static List<EventMarket> validateMarkets(
            List<EventMarket> incomingMarkets
    ) {
        List<EventMarket> copy = List.copyOf(incomingMarkets);

        Set<String> ids = new HashSet<>();

        for (EventMarket market : copy) {
            if (!ids.add(market.id())) {
                throw new IllegalArgumentException(
                        "Duplicate EventMarket ID: " + market.id()
                );
            }
        }

        return copy;
    }

    public static EventUpdateHelper from(Event event) {
        Objects.requireNonNull(event, "Event cannot be null");

        return new EventUpdateHelper(
                event.id(),
                Optional.of(event.name()),
                Optional.of(event.startsAt()),
                Optional.of(event.status()),
                Optional.of(event.markets())
        );
    }
}
