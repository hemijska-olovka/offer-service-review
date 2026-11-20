package com.offer.app.infrastructure.persistence;

import com.offer.app.domain.model.Event;
import com.offer.app.domain.model.Market;

public interface OfferStatePersistence {
    void upsertMarket(Market market);

    void upsertEvent(Event event);
}
