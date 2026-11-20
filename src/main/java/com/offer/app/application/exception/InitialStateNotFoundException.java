package com.offer.app.application.exception;

public final class InitialStateNotFoundException
        extends InitialStateLoadException {

    public InitialStateNotFoundException(String path) {
        super("Initial state file does not exist: " + path);
    }
}
