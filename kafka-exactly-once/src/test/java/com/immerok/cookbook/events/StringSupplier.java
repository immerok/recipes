package com.immerok.cookbook.events;

import com.github.javafaker.Faker;
import java.util.function.Supplier;

/** A supplier that produces Strings. */
public class StringSupplier implements Supplier<String> {
    private final Faker faker = new Faker();

    @Override
    public String get() {
        return faker.lordOfTheRings().character();
    }
}
