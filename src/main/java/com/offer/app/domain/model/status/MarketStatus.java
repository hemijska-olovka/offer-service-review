package com.offer.app.domain.model.status;

public enum MarketStatus implements Status {
    INACTIVE(0),
    ACTIVE(1);

    private final int code;

    MarketStatus(int code) {
        this.code = code;
    }


    public static MarketStatus fromCode(int code) {
        return switch (code){
            case 0 -> INACTIVE;
            case 1 -> ACTIVE;
            default -> throw new IllegalArgumentException("Unknown market status "+code);
        };
    }

    public static MarketStatus requireNonNull(
            MarketStatus status) {
        return Status.requireNonNull(status, MarketStatus.class);
    }

    @Override
    public int getCode() {
        return code;
    }
}