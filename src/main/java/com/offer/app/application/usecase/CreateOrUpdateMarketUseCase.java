package com.offer.app.application.usecase;

import com.offer.app.domain.model.Market;
import com.offer.app.application.repository.MarketRepository;
import org.springframework.stereotype.Component;

import java.util.Objects;
@Component
public class CreateOrUpdateMarketUseCase {
    private final MarketRepository repository;

    public CreateOrUpdateMarketUseCase(
            MarketRepository repository) {
        this.repository = Objects.requireNonNull(repository);
    }

    public void execute(Market market) {
        Objects.requireNonNull(market, "Market is required");
        repository.upsert(market);
    }
}
