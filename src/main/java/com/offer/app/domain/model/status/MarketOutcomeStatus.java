package com.offer.app.domain.model.status;

public enum MarketOutcomeStatus implements Status {
    INACTIVE(0),
    ACTIVE(1);

    private final int code;

    MarketOutcomeStatus(int code) {
        this.code = code;
    }

    public static MarketOutcomeStatus fromCode(int code) {
        return switch (code) {
            case 0 -> INACTIVE;
            case 1 -> ACTIVE;
            default -> throw new IllegalArgumentException(
                    "Unknown MarketOutcomeStatus code: " + code
            );
        };
    }

    @Override
    public int getCode() {
        return code;
    }
    public static MarketOutcomeStatus requireNonNull(
            MarketOutcomeStatus status) {
        return Status.requireNonNull(status, MarketOutcomeStatus.class);
    }
}