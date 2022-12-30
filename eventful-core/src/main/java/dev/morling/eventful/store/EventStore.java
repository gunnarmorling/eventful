package dev.morling.eventful.store;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

public interface EventStore {

    void close();

    void initialize(String streamType);

    Future<Void> persist(String streamType, Event event);

    List<Event> events(String streamType, UUID id);
}
