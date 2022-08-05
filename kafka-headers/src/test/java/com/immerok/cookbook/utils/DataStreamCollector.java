package com.immerok.cookbook.utils;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

/**
 * This class acts as an accessor to elements collected via {@link
 * DataStreamCollectUtil#collectAsync(DataStream, DataStreamCollector)}.
 *
 * @param <T> the element type
 */
public class DataStreamCollector<T> {

    @Nullable private CloseableIterator<T> iterator;

    void setIterator(CloseableIterator<T> iterator) {
        Preconditions.checkArgument(
                this.iterator == null, "A collector can only be used on a single data stream");

        this.iterator = Preconditions.checkNotNull(iterator);
    }

    public CloseableIterator<T> getOutput() {
        Objects.requireNonNull(iterator, "DataStreamCollectUtil#startCollect has not been called.");
        return iterator;
    }
}
