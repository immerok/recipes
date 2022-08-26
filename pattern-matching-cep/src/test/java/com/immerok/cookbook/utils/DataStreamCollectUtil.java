package com.immerok.cookbook.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;

/**
 * This is a utility that allows the collection of any number of streams.
 *
 * <p>This class is heavily based on Flink's {@link DataStream#executeAndCollect()}; it just splits
 * it into a 2 step process to circumvent the API limitation that only 1 stream may be collected.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * StreamExecutionEnvironment env = ...
 * // define workflow, and store the streams to collect in variables
 * ...
 * DataStream<X> streamToCollect = ...
 * ...
 *
 * DataStreamCollectUtil collectUtil = new DataStreamCollectUtil();
 *
 * DataStreamCollector<Event> collector = new DataStreamCollector();
 * collectionUtil.collectAsync(streamToCollect, collector);
 *
 * JobClient jobClient = env.executeAsync();
 * collectionUtil.startCollect(jobClient);
 *
 * while (collector.hasNext()) {
 *     Event nextEvent = collector.next();
 *     // process event
 * }
 * collector.close();
 *
 * }</pre>
 *
 * <p>This will be upstreamed to a future Flink version.
 */
public class DataStreamCollectUtil {

    private final List<CollectResultIterator<?>> iterators = new ArrayList<>();

    /**
     * Prepares the collection of the elements in the given {@link DataStream}. The contained
     * elements can be retrieved via the given {@link DataStreamCollector} once the collection was
     * started via {@link #startCollect}.
     *
     * @param stream stream to collect
     * @param collector collector to feed elements into
     * @param <T> element type
     */
    public <T> void collectAsync(DataStream<T> stream, DataStreamCollector<T> collector) {
        StreamExecutionEnvironment env = stream.getExecutionEnvironment();

        TypeSerializer<T> serializer = stream.getType().createSerializer(env.getConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID().toString();

        CollectSinkOperatorFactory<T> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectSinkOperator<T> operator = (CollectSinkOperator<T>) factory.getOperator();
        CollectResultIterator<T> iterator =
                new CollectResultIterator<>(
                        operator.getOperatorIdFuture(),
                        serializer,
                        accumulatorName,
                        env.getCheckpointConfig());
        CollectStreamSink<T> sink = new CollectStreamSink<>(stream, factory);
        sink.name("Data stream collect sink");
        env.addOperator(sink.getTransformation());

        iterators.add(iterator);
        collector.setIterator(iterator);
    }

    /**
     * Starts the collection of elements, using the provided {@link JobClient}.
     *
     * @param jobClient job client returned by {@link StreamExecutionEnvironment#executeAsync}
     */
    public void startCollect(JobClient jobClient) {
        Objects.requireNonNull(jobClient);
        iterators.forEach(iterator -> iterator.setJobClient(jobClient));
    }
}
