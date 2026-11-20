package com.offer.app.domain.model.status;

public interface Status {
    int getCode();
    static <T extends Enum<T> & Status> T requireNonNull(T status, Class<T> type) {
        if (status == null) {
            throw new IllegalArgumentException(
                    type.getSimpleName() +" cannot be null"
            );
        }

        return status;
    }
}
