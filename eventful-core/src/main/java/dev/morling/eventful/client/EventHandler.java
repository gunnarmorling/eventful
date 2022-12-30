package dev.morling.eventful.client;

import dev.morling.eventful.store.Event;

public interface EventHandler<T> {

    T apply(T aggregate, Event event);
}
