package com.offer.app.domain.model.status;

public enum EventStatus implements Status{
    INACTIVE(0),
    ACTIVE(1);
    private final int code;

    EventStatus(int code) {
        this.code = code;
    }
    public static EventStatus requireNonNull(
            EventStatus status) {
        return Status.requireNonNull(status, EventStatus.class);
    }
    public static EventStatus fromCode(int code) {
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
        return 0;
    }
}
