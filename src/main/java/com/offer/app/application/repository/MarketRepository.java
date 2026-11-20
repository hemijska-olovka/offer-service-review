package com.offer.app.application.repository;

import com.offer.app.domain.model.Market;

import java.util.Optional;

public interface MarketRepository {
    void upsert(Market market);
    Optional<Market> findMarketById(String id);
}
