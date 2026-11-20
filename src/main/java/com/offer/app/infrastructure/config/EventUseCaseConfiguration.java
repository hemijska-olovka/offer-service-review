package com.offer.app.infrastructure.config;

import com.offer.app.application.repository.EventRepository;
import com.offer.app.application.rules.EventMergeRules;
import com.offer.app.application.usecase.CreateOrUpdateEventUseCase;


public class EventUseCaseConfiguration {

    CreateOrUpdateEventUseCase createOrUpdateEventUseCase(EventRepository repository) {
        return new CreateOrUpdateEventUseCase(repository, new EventMergeRules());
    }
}
