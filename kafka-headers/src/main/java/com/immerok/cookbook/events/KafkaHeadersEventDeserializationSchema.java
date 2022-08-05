package com.immerok.cookbook.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaHeadersEventDeserializationSchema
        implements KafkaRecordDeserializationSchema<EnrichedEvent> {

    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    static final String HEADER_TRACE_PARENT = "traceparent";
    static final String HEADER_TRACE_STATE = "tracestate";

    /**
     * For performance reasons it's better to create on ObjectMapper in this open method rather than
     * creating a new ObjectMapper for every record.
     */
    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        // JavaTimeModule is needed for Java 8 data time (Instant) support
        objectMapper = JsonMapper.builder().build().registerModule(new JavaTimeModule());
    }

    /**
     * The deserialize method needs access to the information in the Kafka headers of a
     * KafkaConsumerRecord, therefore we have implemented a KafkaRecordDeserializationSchema
     */
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<EnrichedEvent> out)
            throws IOException {
        final Event event = getEvent(record);
        final Metadata metadata = getMetadata(record);
        final Headers headers = getHeaders(record);
        out.collect(new EnrichedEvent(event, metadata, headers));
    }

    private Event getEvent(ConsumerRecord<byte[], byte[]> record) throws IOException {
        return objectMapper.readValue(record.value(), Event.class);
    }

    /** Extracts the Kafka-provided metadata. */
    private static Metadata getMetadata(ConsumerRecord<byte[], byte[]> record) {
        return new Metadata(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                String.valueOf(record.timestampType()));
    }

    /** Extracts the user-provided headers. */
    private static Headers getHeaders(ConsumerRecord<byte[], byte[]> record) {
        return new Headers(
                getStringHeaderValue(record, HEADER_TRACE_STATE),
                getStringHeaderValue(record, HEADER_TRACE_PARENT));
    }

    private static String getStringHeaderValue(
            ConsumerRecord<byte[], byte[]> record, String header) {
        return new String(record.headers().lastHeader(header).value(), StandardCharsets.UTF_8);
    }

    @Override
    public TypeInformation<EnrichedEvent> getProducedType() {
        return TypeInformation.of(EnrichedEvent.class);
    }
}
