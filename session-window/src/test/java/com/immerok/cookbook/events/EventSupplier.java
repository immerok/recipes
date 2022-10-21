package com.immerok.cookbook.events;

import java.time.Instant;
import java.util.function.Supplier;
import net.datafaker.Faker;

/** A supplier that produces Events. */
public class EventSupplier implements Supplier<Event> {
    private final Faker faker = new Faker();
    private int id = 0;
    private String user = null;
    private int numInteractions = 0;

    @Override
    public Event get() {
        if (numInteractions == 0) {
            user = faker.name().firstName() + " " + faker.name().lastName();
            numInteractions = faker.random().nextInt(10) + 1;
        }
        numInteractions--;
        return new Event(id++, user, Instant.now());
    }
}
