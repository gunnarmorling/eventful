package dev.morling.eventful.store;

public class InvalidCommandException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidCommandException(String message) {
        super(message);
    }
}
