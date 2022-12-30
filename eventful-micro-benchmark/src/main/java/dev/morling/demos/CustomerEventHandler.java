package dev.morling.demos;

import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.morling.eventful.client.EventHandler;
import dev.morling.eventful.store.Event;

public class CustomerEventHandler implements EventHandler<Customer> {

    private ObjectMapper mapper;

    public CustomerEventHandler() {
        this.mapper = new ObjectMapper();
    }

    @Override
    public Customer apply(Customer aggregate, Event event) {
        switch(event.type()) {
            case "created" : return createCustomer(event);
            case "contact-person-added" : return addContactPerson(aggregate, event);
            default: throw new IllegalArgumentException("Unsupported event type %s for stream of type %s".formatted(event.type(), "customer"));
        }

    }

    private Customer createCustomer(Event event) {
        try {
            JsonNode payload = mapper.readTree(event.payload());
            return new Customer(event.aggregateId(), 1, payload.get("name").asText(), new ArrayList<>());
        }
        catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Couldn't deserialize event %s".formatted(event.id()), e);
        }
    }

    private Customer addContactPerson(Customer aggregate, Event event) {
        try {
            JsonNode payload = mapper.readTree(event.payload());
            aggregate.getContactPersons().add(new ContactPerson(payload.get("email").asText(), payload.get("name").asText()));
            aggregate.incrementVersion();
            return aggregate;
        }
        catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Couldn't deserialize event %s".formatted(event.id()), e);
        }
    }

}
