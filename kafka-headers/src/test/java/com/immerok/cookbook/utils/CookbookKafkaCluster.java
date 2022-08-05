package com.immerok.cookbook.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Collections;
import java.util.function.Function;
import java.util.stream.Stream;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.SendKeyValues;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.common.header.Headers;

/** A slim wrapper around <a href="https://mguenther.github.io/kafka-junit/">kafka-junit</a>. */
public class CookbookKafkaCluster extends EmbeddedKafkaCluster {

    private static final ObjectMapper OBJECT_MAPPER =
            JsonMapper.builder().build().registerModule(new JavaTimeModule());

    public CookbookKafkaCluster() {
        super(EmbeddedKafkaClusterConfig.defaultClusterConfig());

        this.start();
    }

    /**
     * Creates a topic with the given name and asynchronously writes all data from the given
     * iterator to that topic.
     *
     * @param topic topic name
     * @param topicData topic data to write
     * @param headerGenerator generates headers for a given event
     * @param <EVENT> event data type
     */
    public <EVENT> void createTopicAsync(
            String topic, Stream<EVENT> topicData, Function<EVENT, Headers> headerGenerator) {
        // eagerly create topic to prevent cases where the job fails because by the time it started
        // no value was written yet.
        createTopic(TopicConfig.withName(topic));
        new Thread(
                        () ->
                                topicData.forEach(
                                        record ->
                                                sendEventAsJSON(
                                                        topic,
                                                        record,
                                                        headerGenerator.apply(record))),
                        "Generator")
                .start();
    }

    /**
     * Sends one JSON-encoded event to the topic and sleeps for 100ms.
     *
     * @param event An event to send to the topic.
     */
    private <EVENT> void sendEventAsJSON(String topic, EVENT event, Headers headers) {
        try {
            SendKeyValues<Object, String> sendRequest =
                    SendKeyValues.to(
                                    topic,
                                    Collections.singleton(
                                            new KeyValue<>(
                                                    null,
                                                    OBJECT_MAPPER.writeValueAsString(event),
                                                    headers)))
                            .build();

            this.send(sendRequest);
            Thread.sleep(100);
        } catch (InterruptedException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
