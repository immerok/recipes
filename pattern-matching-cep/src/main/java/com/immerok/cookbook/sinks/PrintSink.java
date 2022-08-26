package com.immerok.cookbook.sinks;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

/**
 * A printing sink for use in development. Same functionality as the legacy PrintSinkFunction, but
 * uses the new Sink interface.
 *
 * @param <InputT> Type of the elements in the input of the sink; also the type written to its
 *     output
 */
public class PrintSink<InputT> implements Sink<InputT> {

    private static final long serialVersionUID = 1L;
    private final String sinkIdentifier;
    private final boolean stdErr;

    /** Instantiates a print sink function that prints to STDOUT. */
    public PrintSink() {
        this("");
    }

    /**
     * Instantiates a print sink function that prints to STDOUT or STDERR.
     *
     * @param stdErr True, if the format should print to standard error instead of standard out.
     */
    public PrintSink(final boolean stdErr) {
        this("", stdErr);
    }
    /**
     * Instantiates a print sink function that prints to STDOUT and gives a sink identifier.
     *
     * @param sinkIdentifier Message that identifies the sink and is prefixed to the output of the
     *     value
     */
    public PrintSink(final String sinkIdentifier) {
        this(sinkIdentifier, false);
    }

    /**
     * Instantiates a print sink function that prints to STDOUT or STDERR and gives a sink
     * identifier.
     *
     * @param sinkIdentifier Message that identifies the sink and is prefixed to the output of the
     *     value
     * @param stdErr True if the sink should print to STDERR instead of STDOUT.
     */
    public PrintSink(final String sinkIdentifier, final boolean stdErr) {
        this.sinkIdentifier = sinkIdentifier;
        this.stdErr = stdErr;
    }

    @Override
    public SinkWriter<InputT> createWriter(InitContext context) {
        return new PrintSinkWriter<>(sinkIdentifier, stdErr, context);
    }
}
