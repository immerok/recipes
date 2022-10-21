package com.immerok.cookbook.events;

import java.time.Instant;
import java.util.function.Supplier;
import net.datafaker.Faker;

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
