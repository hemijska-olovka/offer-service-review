package com.offer.app.application.usecase;

import com.offer.app.application.repository.OfferStateRepository;
import com.offer.app.domain.model.OfferStateSnapshot;
import com.offer.app.infrastructure.initialstate.InitialStateProvider;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class InitializeOfferStateUseCase {
    private final InitialStateProvider provider;
    private final OfferStateRepository repository;

    public InitializeOfferStateUseCase(
            InitialStateProvider provider,
            OfferStateRepository repository
    ) {
        this.provider = Objects.requireNonNull(provider);
        this.repository = Objects.requireNonNull(repository);
    }

    public void execute() {
        OfferStateSnapshot snapshot = provider.load();
        repository.replaceSnapshot(snapshot);
    }
}
