package com.offer.app.infrastructure.initialstate;

import com.offer.app.domain.model.OfferStateSnapshot;

public interface InitialStateProvider {
    OfferStateSnapshot load();
}
