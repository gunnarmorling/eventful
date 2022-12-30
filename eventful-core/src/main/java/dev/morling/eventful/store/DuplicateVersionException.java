package dev.morling.eventful.store;

import java.util.UUID;

public class DuplicateVersionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String streamType;
    private final UUID aggregateId;
    private final long version;

    public DuplicateVersionException(String streamType, UUID aggregateId, long version) {
        this.streamType = streamType;
        this.aggregateId = aggregateId;
        this.version = version;
    }

    public String streamType() {
        return streamType;
    }

    public UUID aggregateId() {
        return aggregateId;
    }

    public long version() {
        return version;
    }

    @Override
    public String getMessage() {
        return "Duplicate version %s for aggregate %s of type '%s'".formatted(version, aggregateId, streamType);
    }
}
