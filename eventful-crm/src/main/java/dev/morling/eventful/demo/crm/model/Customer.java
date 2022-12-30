package dev.morling.eventful.demo.crm.model;

import java.util.List;
import java.util.UUID;

import dev.morling.eventful.client.CommandResult;
import dev.morling.eventful.store.Event;
import dev.morling.eventful.store.InvalidCommandException;

public class Customer {

    public static record CreateCustomerCommand(String name) {}
    public static record AddContactPersonCommand(String email, String name) {}

    private final UUID id;
    private long version;
    private String name;
    private final List<ContactPerson> contactPersons;

    public Customer(UUID id, long version, String name, List<ContactPerson> contactPersons) {
        this.id = id;
        this.version = version;
        this.name = name;
        this.contactPersons = contactPersons;
    }

    public static CommandResult handle(CreateCustomerCommand command) {
        return new CommandResult(new Event("created", UUID.randomUUID(), 1, "{ \"name\" : \"%s\" }".formatted(command.name())));
    }
    public CommandResult handle(AddContactPersonCommand request) {
        if (contactPersons.size() == 10) {
            throw new InvalidCommandException("No more than ten contact persons can be registered for a customer");
        }

        contactPersons.add(new ContactPerson(request.email(), request.name()));
        version++;

        return new CommandResult(new Event("contact-person-added", id, version, "{ \"email\" : \"%s\", \"name\" : \"%s\" }".formatted(request.email(), request.name())));
    }

    public UUID getId() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    public String getName() {
        return name;
    }

    public List<ContactPerson> getContactPersons() {
        return contactPersons;
    }

    public void incrementVersion() {
        version++;
    }
}
