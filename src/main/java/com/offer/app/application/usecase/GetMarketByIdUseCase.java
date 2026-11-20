package com.offer.app.application.usecase;

import com.offer.app.application.repository.MarketRepository;
import org.springframework.stereotype.Component;

import java.util.Objects;
@Component
public class GetMarketByIdUseCase {
    private final MarketRepository repository;

    public GetMarketByIdUseCase(MarketRepository repository) {
        this.repository = Objects.requireNonNull(repository);
    }

    public void execute(String marketId) {
        repository.findMarketById(marketId);
    }
}
