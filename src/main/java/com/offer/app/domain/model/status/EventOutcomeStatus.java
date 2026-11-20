package com.offer.app.domain.model.status;

public enum EventOutcomeStatus implements Status {
    INACTIVE(0),
    ACTIVE(1);

    private final int code;

    EventOutcomeStatus(int code) {
        this.code = code;
    }


    @Override
    public int getCode() {
        return code;
    }
    public static EventOutcomeStatus requireNonNull(
            EventOutcomeStatus status) {
        return Status.requireNonNull(status, EventOutcomeStatus.class);
    }
}