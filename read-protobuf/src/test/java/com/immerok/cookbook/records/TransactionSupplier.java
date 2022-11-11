package com.immerok.cookbook.records;

import com.immerok.cookbook.Transaction;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Random;
import java.util.function.Supplier;

/** A supplier that produces transactions. */
public class TransactionSupplier implements Supplier<Transaction> {
    private static final int TOTAL_CUSTOMERS = 25;

    private static final Random random = new Random();
    private int id = 0;

    @Override
    public Transaction get() {
        return Transaction.newBuilder()
                .setTTime(Instant.now().toString())
                .setTId(this.id++)
                .setTCustomerId(random.nextInt(TOTAL_CUSTOMERS))
                .setTAmount(BigDecimal.valueOf(1000.0 * random.nextFloat()).doubleValue())
                .build();
    }
}
