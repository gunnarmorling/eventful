package dev.morling.eventful.store;

import java.util.UUID;

public record Event(String type, UUID id, long aggregateId, long version, String payload) {

    public static final long VERSION_INIT = 1;

    public Event {
        if (version < 1) {
            throw new IllegalArgumentException("Version must be larger than 0");
        }
    }

    public Event(String type, long aggregateId, long version, String payload) {
        this(type, UUID.randomUUID(), aggregateId, version, payload);
    }
}
