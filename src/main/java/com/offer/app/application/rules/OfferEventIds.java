package com.offer.app.application.rules;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class OfferEventIds {
    private static final int MAX_IDS = 100;
    private static final int MAX_ID_LENGTH = 128;

    private OfferEventIds() {}

    public static Set<String> parse(List<String> raw) {
        if (raw == null) {
            return null;
        }
        if (raw.isEmpty()) {
            throw new IllegalArgumentException("eventIds must contain at least one ID");
        }

        Set<String> result = new LinkedHashSet<>();
        for (String rawValue : raw) {
            if (rawValue == null) {
                throw new IllegalArgumentException("eventIds must not be null");
            }
            for (String part : rawValue.split(",", -1)) {
                String id = part.trim();
                if (id.isEmpty() || id.length() > MAX_ID_LENGTH ||
                        !id.matches("[A-Za-z0-9._:-]+")) {
                    throw new IllegalArgumentException(
                            "Invalid eventIds value: each ID must be 1-128 characters [A-Za-z0-9._:-]");
                }
                result.add(id);
                if (result.size() > MAX_IDS) {
                    throw new IllegalArgumentException("Too many eventIds (maximum " + MAX_IDS + ")");
                }
            }
        }
        return Set.copyOf(result);
    }
}
