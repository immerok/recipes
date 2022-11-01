package com.immerok.cookbook;

import static java.time.temporal.ChronoUnit.SECONDS;

import com.immerok.cookbook.events.Event;
import java.time.Duration;
import java.util.function.Consumer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

public class ContinuousFileReading {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameters = ParameterTool.fromArgs(args);

        Path inputFolder = new Path(parameters.getRequired("inputFolder"));

        runJob(inputFolder);
    }

    static void runJob(Path dataDirectory) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        defineWorkflow(env, dataDirectory, workflow -> workflow.sinkTo(new PrintSink<>()));
        env.execute();
    }

    static void defineWorkflow(
            StreamExecutionEnvironment env,
            Path dataDirectory,
            Consumer<DataStream<Event>> sinkApplier) {
        CsvReaderFormat<Event> csvFormat = CsvReaderFormat.forPojo(Event.class);
        FileSource<Event> source =
                FileSource.forRecordStreamFormat(csvFormat, dataDirectory)
                        .monitorContinuously(Duration.of(5, SECONDS))
                        .build();

        final DataStreamSource<Event> file =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "File");

        // additional workflow steps go here

        sinkApplier.accept(file);
    }
}
