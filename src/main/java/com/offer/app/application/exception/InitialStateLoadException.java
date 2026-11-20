package com.offer.app.application.exception;

public class InitialStateLoadException extends RuntimeException {
    public InitialStateLoadException(String message) {
        super(message);
    }

    public InitialStateLoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
