package com.offer.app.application.repository;

import com.offer.app.domain.model.Event;

import java.util.Optional;
import java.util.function.Function;

public interface EventRepository {
    Event updateAtomically(
            String eventId,
            Function<Optional<Event>, Event> updater
    );

    Optional<Event> findEventById(String eventId);
}
