package com.offer.app.application.usecase;

import com.offer.app.application.helper.EventUpdateHelper;
import com.offer.app.application.repository.EventRepository;
import com.offer.app.application.rules.EventMergeRules;
import com.offer.app.domain.model.Event;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Optional;
@Component
public class CreateOrUpdateEventUseCase {
    private final EventRepository eventRepository;
    private final EventMergeRules eventMergeRules;

    public CreateOrUpdateEventUseCase(EventRepository eventRepository, EventMergeRules eventMergeRules) {
        this.eventRepository = Objects.requireNonNull(
                eventRepository, "EventRepository is required"
        );

        this.eventMergeRules = Objects.requireNonNull(
                eventMergeRules, "EventMergeRules is required"
        );
    }

    public Event execute(EventUpdateHelper update) {
        Objects.requireNonNull(update, "Event update is required");
        return eventRepository.updateAtomically(
                update.id(), existing -> eventMergeRules.apply(existing, update)
        );
    }
}
