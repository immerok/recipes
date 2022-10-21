package com.immerok.cookbook.events;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import net.datafaker.Faker;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SequenceWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

/** Utils for generating csv files. */
public class DataGenerator {
    private int id = 0;
    private final Faker faker = new Faker();

    public Event next() {
        return new Event(id++, faker.lordOfTheRings().character(), faker.address().cityName());
    }

    /** Writes a single csv file into {@code targetDirectory}, and returns the written elements. */
    public static List<Event> generateFile(File tmpDirectory, File targetDirectory)
            throws IOException {
        return generateFile(tmpDirectory, targetDirectory, 1, new DataGenerator());
    }

    /** Writes several csv file into {@code targetDirectory} asynchronously. */
    public static void generateFilesAsync(File tmpDirectory, File targetDirectory) {
        new Thread(
                        () -> {
                            final DataGenerator dataGenerator = new DataGenerator();
                            // generate 100 files; we limit it to not accidentally fill the disk
                            for (int x = 0; x < 100; x++) {
                                try {
                                    generateFile(
                                            tmpDirectory, targetDirectory, x + 1, dataGenerator);
                                } catch (IOException e) {
                                    return;
                                }
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    return;
                                }
                            }
                        })
                .start();
    }

    private static List<Event> generateFile(
            File tmpDir, File targetDirectory, int fileNumber, DataGenerator dataGenerator)
            throws IOException {
        final Path file =
                Files.createFile(
                        tmpDir.toPath()
                                .resolve(String.format("testData-part%03d.csv", fileNumber)));

        final CsvMapper mapper = new CsvMapper();
        final CsvSchema schema = mapper.schemaFor(Event.class).withoutQuoteChar();

        final List<Event> writtenData = new ArrayList<>();
        try (SequenceWriter sequenceWriter = mapper.writer(schema).writeValues(file.toFile())) {

            for (int x = 0; x < 10; x++) {
                final Event event = dataGenerator.next();
                writtenData.add(event);
                sequenceWriter.write(event);
            }
        }
        Files.move(file, targetDirectory.toPath().resolve(file.getFileName()));
        return writtenData;
    }
}
