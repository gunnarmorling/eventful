package dev.morling.eventful.demo.crm.rest;

import java.util.List;
import java.util.UUID;

import javax.enterprise.event.Observes;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import dev.morling.eventful.client.CommandResult;
import dev.morling.eventful.client.Projector;
import dev.morling.eventful.demo.crm.handler.CustomerEventHandler;
import dev.morling.eventful.demo.crm.model.Customer;
import dev.morling.eventful.demo.crm.model.Customer.AddContactPersonCommand;
import dev.morling.eventful.demo.crm.model.Customer.CreateCustomerCommand;
import dev.morling.eventful.store.Event;
import dev.morling.eventful.store.InvalidCommandException;
import dev.morling.eventful.store.SqliteEventStore;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;

@Path("/customer")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CustomerResource {

    private SqliteEventStore eventStore;

    public void initializeStore(@Observes StartupEvent startupEvent) {
        eventStore = new SqliteEventStore();
        eventStore.initialize("customer");
    }

    public void closeStore(@Observes ShutdownEvent shutdownEvent) {
        eventStore.close();
    }

    @POST
    public Customer createCustomer(CreateCustomerCommand command) throws Exception {
        CommandResult result = Customer.handle(command);
        eventStore.persist("customer", result.event()).get();
        return new Projector().initialize(result.event(), new CustomerEventHandler());
    }

    @POST
    @Path("/{id}/contact")
    public Response addContactPerson(@PathParam("id") UUID id, AddContactPersonCommand command) throws Exception {
        Customer customer = new Projector().restore(eventStore.events("customer", id), new CustomerEventHandler());

        if (customer == null) {
            return Response.status(Status.NOT_FOUND)
                    .entity("Customer with id %s not found".formatted(id))
                    .build();
        }

        try {
            CommandResult result = customer.handle(command);
            eventStore.persist("customer", result.event()).get();
            return Response.ok(customer).build();
        }
        catch(InvalidCommandException e) {
            return Response.status(Status.BAD_REQUEST)
                    .entity(e.getMessage())
                    .build();
        }
    }

    @GET
    @Path("/{id}")
    public Customer getCustomer(@PathParam("id") UUID id) {
        System.out.println("UUID: " + id + " " + System.nanoTime());
        List<Event> events = eventStore.events("customer", id);
        System.out.println("UUID: " + id + " " + System.nanoTime());
        Customer restored = new Projector().restore(events, new CustomerEventHandler());
        System.out.println("UUID: " + id + " " + System.nanoTime());
        return restored;
    }
}
