package com.immerok.cookbook.events;

import java.util.Objects;

/** A collection of Headers metadata provided by Kafka that Flink recognizes as a valid POJO */
public class Metadata {

    /** A Flink POJO must have public fields, or getters and setters */
    public String metadataTopic;

    public long metadataPartition;
    public long metadataOffset;
    public long metadataTimestamp;
    public String metadataTimestampType;

    /** A Flink POJO must have a no-args default constructor */
    public Metadata() {}

    public Metadata(
            final String metadataTopic,
            final long metadataPartition,
            final long metadataOffset,
            final long metadataTimestamp,
            final String metadataTimestampType) {
        this.metadataTopic = metadataTopic;
        this.metadataPartition = metadataPartition;
        this.metadataOffset = metadataOffset;
        this.metadataTimestamp = metadataTimestamp;
        this.metadataTimestampType = metadataTimestampType;
    }

    /** Used for printing during development */
    @Override
    public String toString() {
        return "Metadata{"
                + "topic="
                + metadataTopic
                + ", partition='"
                + metadataPartition
                + '\''
                + ", offset="
                + metadataOffset
                + '\''
                + ", timestamp="
                + metadataTimestamp
                + '\''
                + ", timestampType="
                + metadataTimestampType
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Metadata metadata = (Metadata) o;
        return metadataTopic.equals(metadata.metadataTopic)
                && metadataPartition == metadata.metadataPartition
                && metadataOffset == metadata.metadataOffset
                && metadataTimestamp == metadata.metadataTimestamp
                && metadataTimestampType.equals(metadata.metadataTimestampType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                metadataTopic,
                metadataPartition,
                metadataOffset,
                metadataTimestamp,
                metadataTimestampType);
    }
}
