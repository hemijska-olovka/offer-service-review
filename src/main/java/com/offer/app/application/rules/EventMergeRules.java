package com.offer.app.application.rules;

import com.offer.app.application.helper.EventUpdateHelper;
import com.offer.app.domain.model.Event;
import com.offer.app.domain.model.EventMarket;
import com.offer.app.domain.model.status.EventStatus;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.*;

@Component
public class EventMergeRules {
    public Event apply(
            Optional<Event> current,
            EventUpdateHelper eventUpdateHelper
    ) {
        Objects.requireNonNull(current, "Current event is required");
        Objects.requireNonNull(eventUpdateHelper, "Command is required");

        return current
                .map(event -> merge(event, eventUpdateHelper))
                .orElseGet(() -> create(eventUpdateHelper));
    }

    private Event merge(Event existing, EventUpdateHelper update) {
        if (!existing.id().equals(update.id())) {
            throw new IllegalArgumentException("Event id mismatch");
        }

        Map<String, EventMarket> byId = new LinkedHashMap<>();
        for (EventMarket market : existing.markets()) {
            byId.put(market.id(), market);
        }
        // Incoming market is a FULL replacement including its complete outcome list.
        // Existing markets omitted from the message remain intact.
        update.markets().ifPresent(incoming -> {
            for (EventMarket market : incoming) {
                byId.put(market.id(), market);
            }
        });

        return new Event(
                existing.id(),
                update.name().orElse(existing.name()),
                update.startsAt().orElse(existing.startsAt()),
                update.status().orElse(existing.status()),
                new ArrayList<>(byId.values())
        );
    }

    private Event create(EventUpdateHelper update) {
        String name = update.name().orElseThrow(
                () -> new IllegalArgumentException("Event name is required")
        );

        LocalDateTime startsAt = update.startsAt().orElseThrow(
                () -> new IllegalArgumentException("Event start time is required")
        );

        EventStatus status = update.status().orElseThrow(
                () -> new IllegalArgumentException("Event status is required")
        );

        List<EventMarket> markets = update.markets().orElseThrow(
                () -> new IllegalArgumentException("Event markets are required")
        );

        return new Event(
                update.id(),
                name,
                startsAt,
                status,
                markets
        );
    }
}
