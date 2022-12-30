package dev.morling.eventful.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.morling.eventful.client.EventHandler;
import dev.morling.eventful.store.Event;

public class UserEventHandler implements EventHandler<User> {

    private ObjectMapper mapper;

    public UserEventHandler() {
        this.mapper = new ObjectMapper();
    }

    @Override
    public User apply(User aggregate, Event event) {
        try {
            switch(event.type()) {
                case "user-created": return mapper.readValue(event.payload(), User.class);
                case "email-updated" : return new User(mapper.readTree(event.payload()).get("email").asText(), aggregate.name());
                default: throw new IllegalArgumentException("Unsupported event type %s for stream of type %s".formatted(event.type(), "user"));
            }
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException("Couldn't replay event %s".formatted(event.id()), e);
        }
    }
}
