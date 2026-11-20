package com.offer.app.domain.model.status;

public enum EventMarketOutcomeStatus implements Status {
    INACTIVE(0),
    ACTIVE(1);

    private final int code;

    EventMarketOutcomeStatus(int code) {
        this.code = code;
    }

    public static EventMarketOutcomeStatus fromCode(int code) {
        return switch (code) {
            case 0 -> INACTIVE;
            case 1 -> ACTIVE;
            default -> throw new IllegalArgumentException(
                    "Unknown EventStatus code: " + code
            );
        };
    }

    @Override
    public int getCode() {
        return code;
    }
    public static EventMarketOutcomeStatus requireNonNull(
            EventMarketOutcomeStatus status) {
        return Status.requireNonNull(status, EventMarketOutcomeStatus.class);
    }
}