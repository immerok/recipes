package com.immerok.cookbook.records;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.stream.Stream;

/** A supplier for Transactions test data. */
public class TransactionTestDataSupplier {

    public static Stream<Transaction> getTestData() {
        return Stream.of(
                new Transaction(
                        Instant.parse("2021-10-08T12:33:12.000Z"),
                        1L,
                        12L,
                        new BigDecimal("325.12")),
                new Transaction(
                        Instant.parse("2021-10-10T08:00:00.000Z"), 2L, 7L, new BigDecimal("13.99")),
                new Transaction(
                        Instant.parse("2021-10-10T08:00:00.000Z"), 2L, 7L, new BigDecimal("13.99")),
                new Transaction(
                        Instant.parse("2021-10-14T17:04:00.000Z"),
                        3L,
                        12L,
                        new BigDecimal("52.48")),
                new Transaction(
                        Instant.parse("2021-10-14T17:06:00.000Z"),
                        4L,
                        32L,
                        new BigDecimal("26.11")),
                new Transaction(
                        Instant.parse("2021-10-14T18:23:00.000Z"),
                        5L,
                        32L,
                        new BigDecimal("22.03")));
    }
}
