package com.offer.app.application.repository;

import com.offer.app.domain.model.OfferStateSnapshot;

public interface OfferStateRepository {
    void replaceSnapshot(OfferStateSnapshot snapshot);
    OfferStateSnapshot loadSnapshot();
}
