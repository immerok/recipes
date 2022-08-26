package com.immerok.cookbook;

import static com.immerok.cookbook.PatternMatchingCEP.TOPIC;

import com.immerok.cookbook.extensions.FlinkMiniClusterExtension;
import com.immerok.cookbook.patterns.MatcherV1;
import com.immerok.cookbook.patterns.MatcherV2;
import com.immerok.cookbook.patterns.MatcherV3;
import com.immerok.cookbook.patterns.PatternMatcher;
import com.immerok.cookbook.records.OscillatingSensorReadingSupplier;
import com.immerok.cookbook.records.SensorReading;
import com.immerok.cookbook.utils.CookbookKafkaCluster;
import java.time.Duration;
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * These production tests each run their respective PatternMatcher against an oscillating event
 * source that produces 5 seconds of HOT readings followed by 5 seconds of cool readings, over and
 * over.
 *
 * <p>The various matchers are all configured to match hot streaks that are 1 second long.
 *
 * <p>The original event stream is printed to STDOUT and events that conclude a pattern match are
 * also printed to STDERR.
 */
@ExtendWith(FlinkMiniClusterExtension.class)
class ProductionJobTests {

    private static final int EVENTS_PER_SECOND = 1;

    /**
     * Runs the production job with MatcherV1 against an in-memory Kafka cluster.
     *
     * <p>This is a manual test because this job will never finish.
     *
     * <p>You should see the job match more than once during each 5-second hot streak, and it will
     * sometimes (erroneously) complete a match with the first HOT event in a streak.
     */
    @Test
    @Disabled("Not running 'testProductionJobWithMatcherV1()' because it is a manual test.")
    void testProductionJobWithMatcherV1() throws Exception {
        Duration limitOfHeatTolerance = Duration.ofSeconds(1);
        int secondsOfHeat = 5;
        runProductionJob(new MatcherV1(), secondsOfHeat, limitOfHeatTolerance);
    }

    /**
     * Runs the production job with MatcherV2 against an in-memory Kafka cluster.
     *
     * <p>This is a manual test because this job will never finish.
     *
     * <p>You should see the job match more than once during each 5-second hot streak.
     */
    @Test
    @Disabled("Not running 'testProductionJobWithMatcherV2()' because it is a manual test.")
    void testProductionJobWithMatcherV2() throws Exception {
        Duration limitOfHeatTolerance = Duration.ofSeconds(1);
        int secondsOfHeat = 5;
        runProductionJob(new MatcherV2(), secondsOfHeat, limitOfHeatTolerance);
    }

    /**
     * Runs the production job with MatcherV3 against an in-memory Kafka cluster.
     *
     * <p>This is a manual test because this job will never finish.
     *
     * <p>You should see the job match exactly once during every 5-second hot streak.
     */
    @Test
    @Disabled("Not running 'testProductionJobWithMatcherV3()' because it is a manual test.")
    void testProductionJobWithMatcherV3() throws Exception {
        Duration limitOfHeatTolerance = Duration.ofSeconds(1);
        int secondsOfHeat = 5;
        runProductionJob(new MatcherV3(), secondsOfHeat, limitOfHeatTolerance);
    }

    private static void runProductionJob(
            PatternMatcher<SensorReading, SensorReading> matcher,
            int secondsOfHeat,
            Duration limitOfHeatTolerance)
            throws Exception {

        try (final CookbookKafkaCluster kafka = new CookbookKafkaCluster(EVENTS_PER_SECOND)) {
            kafka.createTopicAsync(
                    TOPIC, Stream.generate(new OscillatingSensorReadingSupplier(secondsOfHeat)));

            PatternMatchingCEP.runJob(matcher, limitOfHeatTolerance);
        }
    }
}
