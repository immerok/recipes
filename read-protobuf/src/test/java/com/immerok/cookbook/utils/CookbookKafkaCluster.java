package com.immerok.cookbook.utils;

import java.io.UnsupportedEncodingException;
import java.util.stream.Stream;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.SendValues;
import net.mguenther.kafka.junit.TopicConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

/** A slim wrapper around <a href="https://mguenther.github.io/kafka-junit/">kafka-junit</a>. */
public class CookbookKafkaCluster extends EmbeddedKafkaCluster {

    public CookbookKafkaCluster() {
        super(EmbeddedKafkaClusterConfig.defaultClusterConfig());

        this.start();
    }

    /**
     * Creates a topic with the given name and synchronously writes all data from the given stream
     * to that topic.
     *
     * @param topic topic to create
     * @param topicData data to write
     */
    public void createTopic(String topic, Stream<byte[]> topicData) {
        createTopic(TopicConfig.withName(topic));
        topicData.forEach(
                record -> {
                    try {
                        sendEvent(topic, record);
                    } catch (UnsupportedEncodingException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * Creates a topic with the given name and asynchronously writes all data from the given stream
     * to that topic.
     *
     * @param topic topic to create
     * @param topicData data to write
     */
    public void createTopicAsync(String topic, Stream<byte[]> topicData) {
        createTopic(TopicConfig.withName(topic));
        new Thread(
                        () ->
                                topicData.forEach(
                                        record -> {
                                            try {
                                                sendEvent(topic, record);
                                            } catch (UnsupportedEncodingException e) {
                                                throw new RuntimeException(e);
                                            }
                                        }),
                        "Generator")
                .start();
    }

    /**
     * Sends one event to the topic and sleeps for 100ms.
     *
     * @param event An event to send to the topic.
     */
    private void sendEvent(String topic, byte[] event) throws UnsupportedEncodingException {
        try {
            final SendValues<byte[]> sendRequest =
                    SendValues.to(topic, event)
                            .with(
                                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                    "org.apache.kafka.common.serialization.ByteArraySerializer")
                            .build();
            this.send(sendRequest);
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
