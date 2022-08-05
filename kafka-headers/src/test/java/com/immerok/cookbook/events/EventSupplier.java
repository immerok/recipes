package com.immerok.cookbook.events;

import com.github.javafaker.Faker;
import java.time.Instant;
import java.util.function.Supplier;

/** A supplier that produces Events. */
public class EventSupplier implements Supplier<Event> {
    private final Faker faker = new Faker();
    private int id = 0;

    @Override
    public Event get() {
        String lotrCharacter = faker.lordOfTheRings().character();
        return new Event(id++, lotrCharacter, Instant.now());
    }
}
