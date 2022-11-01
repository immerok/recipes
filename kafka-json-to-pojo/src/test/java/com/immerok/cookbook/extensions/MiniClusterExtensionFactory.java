package com.immerok.cookbook.extensions;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.MiniClusterExtension;

/** Convenience class to create {@link MiniClusterExtension} with different configurations. */
public final class MiniClusterExtensionFactory {

    private static final int PARALLELISM = 2;

    public static MiniClusterExtension withDefaultConfiguration() {
        return new MiniClusterExtension(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(PARALLELISM)
                        .setNumberTaskManagers(1)
                        .build());
    }

    private MiniClusterExtensionFactory() {}
}
