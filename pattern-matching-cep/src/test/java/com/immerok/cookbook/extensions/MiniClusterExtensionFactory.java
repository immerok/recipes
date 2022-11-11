package com.immerok.cookbook.extensions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration.Builder;
import org.apache.flink.test.junit5.MiniClusterExtension;

/** Convenience class to create {@link MiniClusterExtension} with different configurations. */
public final class MiniClusterExtensionFactory {

    private static final int PARALLELISM = 2;

    public static MiniClusterExtension withDefaultConfiguration() {
        return new MiniClusterExtension(defaultBuilder().build());
    }

    public static MiniClusterExtension withCustomConfiguration(Configuration configuration) {
        return new MiniClusterExtension(defaultBuilder().setConfiguration(configuration).build());
    }

    private MiniClusterExtensionFactory() {}

    private static Builder defaultBuilder() {
        return new MiniClusterResourceConfiguration.Builder()
                .setNumberSlotsPerTaskManager(PARALLELISM)
                .setNumberTaskManagers(1);
    }
}
