package com.offer.app.domain.model;

import com.offer.app.domain.model.status.EventStatus;

import java.time.LocalDateTime;
import java.util.List;

public record Event(
        String id,
        String name,
        LocalDateTime startsAt,
        EventStatus status,
        List<EventMarket> markets
) {
    public Event {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException(
                    "Event ID is required"
            );
        }

        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException(
                    "Event name is required"
            );
        }

        EventStatus.requireNonNull(status);
    }
}
