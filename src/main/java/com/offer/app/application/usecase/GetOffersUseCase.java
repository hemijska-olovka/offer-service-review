package com.offer.app.application.usecase;

import com.offer.app.application.dto.OfferResponse;
import com.offer.app.application.mapper.OfferResponseAssembler;
import com.offer.app.application.repository.OfferStateRepository;
import com.offer.app.application.rules.OfferEventIds;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

@Component
public class GetOffersUseCase {
    private final OfferStateRepository repository;
    private final OfferResponseAssembler assembler;

    public GetOffersUseCase(OfferStateRepository repository, OfferResponseAssembler assembler) {
        this.repository = Objects.requireNonNull(repository);
        this.assembler = Objects.requireNonNull(assembler);
    }

    public OfferResponse execute(List<String> requestedEventIds) {
        var eventIds = OfferEventIds.parse(requestedEventIds);

        var snapshot = repository.loadSnapshot();

        return assembler.assemble(
                snapshot.events(),
                snapshot.markets(),
                eventIds
        );
    }
}
