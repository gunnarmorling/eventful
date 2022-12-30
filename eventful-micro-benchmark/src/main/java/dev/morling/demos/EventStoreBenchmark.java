/*
 *  Copyright 2022 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.demos;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import dev.morling.eventful.client.Projector;
import dev.morling.eventful.store.Event;
import dev.morling.eventful.store.SqliteEventStore;

public class EventStoreBenchmark {

    private static final int CUSTOMERS = 20_000;
    private static final int CONTACTS_PER_CUSTOMER = 50;

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        SqliteEventStore eventStore;
        String[] values;
        UUID[] ids;

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            Random random = new Random();
            values = random.ints(CUSTOMERS, 0, 100_000)
                    .mapToObj(i -> i + "_ACME, Inc.")
                    .toArray(String[]::new);

            ids = new UUID[values.length];
            eventStore = new SqliteEventStore();
            eventStore.initialize("customer_write");
            eventStore.initialize("customer_read");

            Future<Void> result = null;
            for(int i = 0; i < CUSTOMERS; i++) {
                UUID customerId = UUID.randomUUID();
                ids[i] = customerId;
                result = eventStore.persist("customer_read", new Event("created", customerId, 1, "{ \"name\" : \"" + values[i] + "\" }"));

                for(int j = 0; j < CONTACTS_PER_CUSTOMER; j++) {
                    result = eventStore.persist("customer_read", new Event("contact-person-added", customerId, j + 2, "{ \"email\" : \"bobby" + j + "@acme.com\", \"name\" : \"Bobby" + j + "\" }"));
                }
            }

            result.get();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            eventStore.close();
        }
    }

//    @Benchmark
//    @BenchmarkMode(Mode.Throughput)
//    @OperationsPerInvocation(CUSTOMERS)
//    public void singleInsert(BenchmarkState state, Blackhole blackhole) throws Exception {
//        String[] customers = state.values;
//        for (String customer : customers) {
//            Void result = state.eventStore.persist("customer_write", new Event("created", UUID.randomUUID(), 1, "{ \"name\" : \"" + customer + "\" }")).get();
//            blackhole.consume(result);
//        }
//    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(CUSTOMERS)
    public void bulkInsert(BenchmarkState state, Blackhole blackhole) throws Exception {
        String[] customers = state.values;
        Future<Void> result = null;
        for (String customer : customers) {
            result = state.eventStore.persist("customer_write", new Event("created", UUID.randomUUID(), 1, "{ \"name\" : \"" + customer + "\" }"));
        }

        result.get();
        blackhole.consume(result);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(CUSTOMERS)
    public void read(BenchmarkState state, Blackhole blackhole) throws Exception {
        for(int i = 0; i < CUSTOMERS; i++) {
            List<Event> events = state.eventStore.events("customer_read", state.ids[i]);
            Customer customer = new Projector().restore(events, new CustomerEventHandler());
            blackhole.consume(customer);
        }
    }

    public static void main(String[] args) throws Exception {
        SqliteEventStore eventStore = new SqliteEventStore();
        eventStore.initialize("customer");
        int COUNT = 100_000;
        UUID[] ids = new UUID[COUNT];
        Random random = new Random();

        Future<Void> result = null;
        for(int i = 0; i < COUNT; i++) {
            long before = System.nanoTime();
            UUID customerId = UUID.randomUUID();
            ids[i] = customerId;
            result = eventStore.persist("customer", new Event("created", customerId, 1, "{ \"name\" : \"" + i + "_ACME, Inc." + "\" }"));

            for(int j = 0; j < 50; j++) {
                result = eventStore.persist("customer", new Event("contact-person-added", customerId, j + 2, """
                        {
                          "email" : "bobby%1$s@acme.com",
                          "name" : "Bobby%1$s",
                          "email2" : "bobby%1$s@acme.com",
                          "name2" : "Bobby%1$s",
                          "email3" : "bobby%1$s@acme.com",
                          "name3" : "Bobby%1$s",
                          "email4" : "bobby%1$s@acme.com",
                          "name4" : "Bobby%1$s",
                          "email5" : "bobby%1$s@acme.com",
                          "name5" : "Bobby%1$s",
                          "email6" : "bobby%1$s@acme.com",
                          "name6" : "Bobby%1$s",
                          "email7" : "bobby%1$s@acme.com",
                          "name7" : "Bobby%1$s",
                          "email8" : "bobby%1$s@acme.com",
                          "name8" : "Bobby%1$s",
                          "email9" : "bobby%1$s@acme.com",
                          "name9" : "Bobby%1$s",
                          "email10" : "bobby%1$s@acme.com",
                          "name10" : "Bobby%1$s",
                          "email11" : "bobby%1$s@acme.com",
                          "name11" : "Bobby%1$s",
                          "email12" : "bobby%1$s@acme.com",
                          "name12" : "Bobby%1$s",
                          "email13" : "bobby%1$s@acme.com",
                          "name13" : "Bobby%1$s",
                          "email14" : "bobby%1$s@acme.com",
                          "name14" : "Bobby%1$s",
                          "email15" : "bobby%1$s@acme.com",
                          "name15" : "Bobby%1$s",
                          "email16" : "bobby%1$s@acme.com",
                          "name16" : "Bobby%1$s",
                          "email17" : "bobby%1$s@acme.com",
                          "name17" : "Bobby%1$s",
                          "email18" : "bobby%1$s@acme.com",
                          "name18" : "Bobby%1$s"
                        }
                        """.formatted(j)));
            }


            if (i % 10_000 == 0) {
                result.get();
                long after = System.nanoTime();
                System.out.println(i);
                System.out.println(after - before);
            }
        }

        result.get();

        for(int i = 0; i < 100_000; i++) {
            long before = System.nanoTime();
            List<Event> events = eventStore.events("customer", ids[random.nextInt(0, COUNT)]);
            Customer customer = new Projector().restore(events, new CustomerEventHandler());
            long after = System.nanoTime();
            if (i % 10_000 == 0) {
                System.out.println(after - before);
            }
        }


        eventStore.close();
    }
}
