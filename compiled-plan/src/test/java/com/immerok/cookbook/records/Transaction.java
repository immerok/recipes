package com.immerok.cookbook.records;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

public class Transaction {

    /**
     * Without this annotation, the timestamps are serialized like this:
     * {"t_time":1658419083.146222000, ...} <br>
     * and the Kafka connector used with the Table API won't be able to deserialize this.
     */
    @JsonFormat(
            shape = JsonFormat.Shape.STRING,
            pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            timezone = "UTC")
    public Instant t_time;

    public long t_id;
    public long t_customer_id;
    public BigDecimal t_amount;

    public Transaction() {}

    public Transaction(Instant t_time, long t_id, long t_customer_id, BigDecimal t_amount) {
        this.t_time = t_time;
        this.t_id = t_id;
        this.t_customer_id = t_customer_id;
        this.t_amount = t_amount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return t_id == that.t_id
                && t_customer_id == that.t_customer_id
                && t_time.equals(that.t_time)
                && t_amount.equals(that.t_amount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(t_time, t_id, t_customer_id, t_amount);
    }

    @Override
    public String toString() {
        return "Transaction("
                + "t_time="
                + t_time
                + ", t_id="
                + t_id
                + ", t_customer_id="
                + t_customer_id
                + ", t_amount="
                + t_amount
                + ')';
    }
}
