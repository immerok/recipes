package com.immerok.cookbook.records;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import javax.annotation.Nullable;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

/**
 * Flink lacks a JsonInputFormat to use with the {@code FileSource}. In its absence, this is a
 * custom input format for JSON that only handles POJOs. It includes support for the {@code
 * JavaTimeModule} which is required for reading ISO-8601-encoded date-time strings.
 */
public class JsonPojoInputFormat<T> extends SimpleStreamFormat<T> {
    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_CHARSET_NAME = "UTF-8";

    private final Class<T> klass;

    public JsonPojoInputFormat(Class<T> klass) {
        this.klass = klass;
    }

    @Override
    public Reader<T> createReader(Configuration config, FSDataInputStream stream)
            throws IOException {
        final BufferedReader reader =
                new BufferedReader(new InputStreamReader(stream, DEFAULT_CHARSET_NAME));
        return new JsonReader<>(reader, klass);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(klass);
    }

    // ------------------------------------------------------------------------

    /** The actual reader for the {@code JsonPojoInputFormat}. */
    @PublicEvolving
    public static final class JsonReader<T> implements StreamFormat.Reader<T> {

        private final ObjectMapper objectMapper;
        private final BufferedReader reader;
        private final Class<T> klass;

        JsonReader(final BufferedReader reader, Class<T> klass) {
            // needed for JSR-310 / ISO-8601 input, e.g., "2022-07-19T12:00:00.000Z"
            this.objectMapper = JsonMapper.builder().build().registerModule(new JavaTimeModule());
            this.reader = reader;
            this.klass = klass;
        }

        @Nullable
        @Override
        public T read() throws IOException {
            String line = reader.readLine();
            if (line == null) {
                return null;
            }
            return objectMapper.readValue(line, klass);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
