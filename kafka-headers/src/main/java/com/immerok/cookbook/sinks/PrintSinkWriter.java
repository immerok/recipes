package com.immerok.cookbook.sinks;

import java.io.PrintStream;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

/** A SinkWriter for the PrintSink. Similar to the legacy PrintSinkOutputWriter. */
@Internal
public class PrintSinkWriter<InputT> implements SinkWriter<InputT> {

    private final transient PrintStream stream;
    private final transient String completedPrefix;

    PrintSinkWriter(String sinkIdentifier, boolean stdErr, Sink.InitContext context) {
        this.stream = stdErr ? System.err : System.out;

        int subtaskIndex = context.getSubtaskId();
        int numParallelSubtasks = context.getNumberOfParallelSubtasks();
        String prefix = sinkIdentifier;

        if (numParallelSubtasks > 1) {
            if (!prefix.isEmpty()) {
                prefix += ":";
            }
            prefix += (subtaskIndex + 1);
        }

        if (!prefix.isEmpty()) {
            prefix += "> ";
        }

        this.completedPrefix = prefix;
    }

    @Override
    public void write(InputT record, Context context) {
        stream.println(completedPrefix + record.toString());
    }

    @Override
    public void flush(boolean endOfInput) {}

    @Override
    public void close() {}
}
