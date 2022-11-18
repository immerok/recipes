package com.immerok.cookbook.events;

import java.math.BigDecimal;
import java.util.Objects;

public class Transaction {

    // A Flink POJO must have public fields, or getters and setters
    public int t_id;
    public int t_customer_id;
    public BigDecimal t_amount;

    // A Flink POJO must have a no-args default constructor
    public Transaction() {}

    public Transaction(final int t_id, final int t_customer_id, final BigDecimal t_amount) {
        this.t_id = t_id;
        this.t_customer_id = t_customer_id;
        this.t_amount = t_amount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Transaction transaction = (Transaction) o;
        return t_id == transaction.t_id
                && t_customer_id == transaction.t_customer_id
                && t_amount.equals(transaction.t_amount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(t_id, t_customer_id, t_amount);
    }

    @Override
    public String toString() {
        return "Transaction{"
                + "t_id="
                + t_id
                + ", t_customer_id="
                + t_customer_id
                + ", t_amount="
                + t_amount
                + '}';
    }
}
