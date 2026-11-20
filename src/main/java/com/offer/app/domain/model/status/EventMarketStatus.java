package com.offer.app.domain.model.status;

public enum EventMarketStatus implements Status {
    INACTIVE(0),
    ACTIVE(1);

    private final int code;

    EventMarketStatus(int code) {
        this.code = code;
    }
    public static EventMarketStatus fromCode(int code) {
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
    public static EventMarketStatus requireNonNull(
            EventMarketStatus status) {
        return Status.requireNonNull(status, EventMarketStatus.class);
    }
}