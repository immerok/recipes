package com.immerok.cookbook.records;

import com.immerok.cookbook.events.Transaction;
import java.math.BigDecimal;
import java.util.Random;
import java.util.function.Supplier;

public class StatementSupplier implements Supplier<Transaction> {
    private static final int TOTAL_CUSTOMERS = 25;

    private static final Random random = new Random();
    private int id = 1;

    @Override
    public Transaction get() {
        int t_customer_id = random.nextInt(TOTAL_CUSTOMERS);
        BigDecimal t_amount = new BigDecimal(1000.0 * random.nextFloat());
        return new Transaction(id++, t_customer_id, t_amount);
    }
}
