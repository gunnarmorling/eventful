package dev.morling.eventful.client;

import java.util.List;

import dev.morling.eventful.store.Event;

public class Projector {

    public <T> T restore(List<Event> events, EventHandler<T> eventHandler) {
        T aggregate = null;

        for (Event event : events) {
            aggregate = eventHandler.apply(aggregate, event);
        }

        return aggregate;
    }

    public <T> T initialize(Event event, EventHandler<T> eventHandler) {
        return eventHandler.apply(null, event);
    }

    public <T> T apply(Event event, EventHandler<T> eventHandler, T aggregate) {
        return eventHandler.apply(aggregate, event);
    }
}
