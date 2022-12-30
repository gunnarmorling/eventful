package dev.morling.eventful.test;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.morling.eventful.client.CommandResult;
import dev.morling.eventful.client.Projector;
import dev.morling.eventful.store.EventStore;
import dev.morling.eventful.store.SqliteEventStore;
import dev.morling.eventful.test.handler.CustomerEventHandler;
import dev.morling.eventful.test.model.ContactPerson;
import dev.morling.eventful.test.model.Customer;
import dev.morling.eventful.test.model.Customer.AddContactPersonCommand;
import dev.morling.eventful.test.model.Customer.CreateCustomerCommand;

public class EventStoreTest {

    private EventStore eventStore;

    @BeforeEach
    public void setupStore() {
        eventStore = new SqliteEventStore();
    }

    @AfterEach
    public void closeStore() {
        eventStore.close();
    }

    @Test
    public void shouldPersistAndPlaybackEvents() throws Exception {
        eventStore.initialize("customer");
        Projector projector = new Projector();

        CreateCustomerCommand command = new CreateCustomerCommand("ACME, Inc.");
        CommandResult result = Customer.handle(command);
        eventStore.persist("customer", result.event()).get();

        Customer customer = projector.initialize(result.event(), new CustomerEventHandler());
        assertThat(customer.getName()).isEqualTo("ACME, Inc.");

        result = customer.handle(new AddContactPersonCommand("bob@acme.com", "Bob"));
        eventStore.persist("customer", result.event()).get();

        result = customer.handle(new AddContactPersonCommand("sarah@acme.com", "Sarah"));
        eventStore.persist("customer", result.event()).get();

        Customer restored = projector.restore(eventStore.events("customer", customer.getId()), new CustomerEventHandler());
        assertThat(restored.getId()).isEqualTo(customer.getId());
        assertThat(restored.getName()).isEqualTo("ACME, Inc.");
        assertThat(restored.getVersion()).isEqualTo(3);
        assertThat(restored.getContactPersons()).containsExactly(new ContactPerson("bob@acme.com", "Bob"), new ContactPerson("sarah@acme.com", "Sarah"));
    }
}
