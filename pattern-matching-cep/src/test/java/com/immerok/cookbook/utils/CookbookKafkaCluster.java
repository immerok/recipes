package com.immerok.cookbook.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.stream.Stream;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.SendValues;
import net.mguenther.kafka.junit.TopicConfig;

/** A slim wrapper around <a href="https://mguenther.github.io/kafka-junit/">kafka-junit</a>. */
public class CookbookKafkaCluster extends EmbeddedKafkaCluster {

    private static final int DEFAULT_EVENTS_PER_SECOND = 10;
    private final int eventsPerSecond;

    private static final ObjectMapper OBJECT_MAPPER =
            JsonMapper.builder().build().registerModule(new JavaTimeModule());

    public CookbookKafkaCluster() {
        super(EmbeddedKafkaClusterConfig.defaultClusterConfig());
        this.eventsPerSecond = DEFAULT_EVENTS_PER_SECOND;
        this.start();
    }

    public CookbookKafkaCluster(int eventsPerSecond) {
        super(EmbeddedKafkaClusterConfig.defaultClusterConfig());
        this.eventsPerSecond = eventsPerSecond;
        this.start();
    }

    /**
     * Creates a topic with the given name and synchronously writes all data from the given stream
     * to that topic.
     *
     * @param topic topic to create
     * @param topicData data to write
     * @param <EVENT> event type
     */
    public <EVENT> void createTopic(String topic, Stream<EVENT> topicData) {
        createTopic(TopicConfig.withName(topic));
        topicData.forEach(record -> sendEventAsJSON(topic, record));
    }

    /**
     * Creates a topic with the given name and asynchronously writes all data from the given stream
     * to that topic.
     *
     * @param topic topic to create
     * @param topicData data to write
     * @param <EVENT> event type
     */
    public <EVENT> void createTopicAsync(String topic, Stream<EVENT> topicData) {
        createTopic(TopicConfig.withName(topic));
        new Thread(() -> topicData.forEach(record -> sendEventAsJSON(topic, record)), "Generator")
                .start();
    }

    /**
     * Sends one JSON-encoded event to the topic and sleeps for 100ms.
     *
     * @param event An event to send to the topic.
     */
    private <EVENT> void sendEventAsJSON(String topic, EVENT event) {
        try {
            final SendValues<String> sendRequest =
                    SendValues.to(topic, OBJECT_MAPPER.writeValueAsString(event)).build();
            this.send(sendRequest);
            Thread.sleep(1000 / eventsPerSecond);
        } catch (InterruptedException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
