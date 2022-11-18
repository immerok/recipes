package com.immerok.cookbook;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.immerok.cookbook.events.Transaction;
import com.immerok.cookbook.extensions.MiniClusterExtensionFactory;
import com.immerok.cookbook.records.StatementSupplier;
import com.immerok.cookbook.utils.CookbookPostgresCluster;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ReadCDCTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK =
            MiniClusterExtensionFactory.withDefaultConfiguration();

    @RegisterExtension
    static final CookbookPostgresCluster POSTGRES = new CookbookPostgresCluster();

    @BeforeEach
    void setUp() throws SQLException {
        POSTGRES.executeStatement("CREATE schema transactions");
        POSTGRES.executeStatement(
                "CREATE TABLE transactions.incoming ("
                        + "t_id serial PRIMARY KEY,"
                        + "t_customer_id serial,"
                        + "t_amount REAL)");
        POSTGRES.executeStatement("ALTER TABLE transactions.incoming REPLICA IDENTITY FULL");
    }

    @AfterEach
    void tearDown() throws SQLException {
        POSTGRES.executeStatement("DROP TABLE transactions.incoming");
        POSTGRES.executeStatement("DROP SCHEMA transactions");
    }

    @Test
    @Disabled("Not running 'testProductionJob()' because it is a manual test.")
    void testProductionJob() throws Exception {
        Stream<Transaction> testData = Stream.generate(new StatementSupplier());
        sendTestData(testData);

        ReadCDC.runJob();
    }

    @Test
    void JobProducesExpectedNumberOfResults() throws Exception {
        SourceFunction<String> source =
                PostgreSQLSource.<String>builder()
                        .hostname("localhost")
                        .port(5432)
                        .database("postgres")
                        .schemaList("transactions")
                        .tableList("transactions.incoming")
                        .username("postgres")
                        .password("postgres")
                        // Set the logical decoding plugin to pgoutput which
                        // is used by the embedded postgres database
                        .decodingPluginName("pgoutput")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        POSTGRES.executeStatement("INSERT INTO transactions.incoming VALUES (1,24,99.08)");
        POSTGRES.executeStatement("INSERT INTO transactions.incoming VALUES (2,7,405.01)");
        POSTGRES.executeStatement("INSERT INTO transactions.incoming VALUES (3,24,974.90)");
        POSTGRES.executeStatement("INSERT INTO transactions.incoming VALUES (4,8,100.19)");
        POSTGRES.executeStatement("INSERT INTO transactions.incoming VALUES (5,24,161.48)");

        final DataStream.Collector<String> testSink = new DataStream.Collector<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ReadCDC.defineWorkflow(env, source, workflow -> workflow.collectAsync(testSink));
        env.executeAsync();

        final List<String> data = new ArrayList();
        try (final CloseableIterator<String> collected = testSink.getOutput()) {
            for (int x = 0; x < 5 && collected.hasNext(); x++) {
                data.add(collected.next());
            }
        }

        ObjectMapper objectMapper = new ObjectMapper();
        ArrayList<Transaction> transaction = new ArrayList<>();
        for (String datum : data) {
            JsonNode jsonNode = objectMapper.readTree(datum);
            transaction.add(objectMapper.treeToValue(jsonNode.at("/after"), Transaction.class));
        }

        assertThat(transaction)
                .containsExactly(
                        new Transaction(1, 24, new BigDecimal("99.08")),
                        new Transaction(2, 7, new BigDecimal("405.01")),
                        new Transaction(3, 24, new BigDecimal("974.9")),
                        new Transaction(4, 8, new BigDecimal("100.19")),
                        new Transaction(5, 24, new BigDecimal("161.48")));
    }

    private static void sendTestData(Stream<Transaction> testData) {
        new Thread(
                        () ->
                                testData.forEach(
                                        record -> {
                                            try {
                                                executeTestDataStatements(record);
                                            } catch (InterruptedException e) {
                                                throw new RuntimeException(e);
                                            } catch (SQLException e) {
                                                throw new RuntimeException(e);
                                            }
                                        }),
                        "Generator")
                .start();
    }

    private static void executeTestDataStatements(Transaction record)
            throws InterruptedException, SQLException {
        if (record.t_id % 5 == 0) {
            // For every 5th record, update the previous one with an update amount
            final int idPreviousInsert = record.t_id - 1;
            POSTGRES.executeStatement(
                    "UPDATE transactions.incoming SET t_amount = "
                            + record.t_amount
                            + "WHERE t_id="
                            + idPreviousInsert);
        }
        POSTGRES.executeStatement(
                "INSERT INTO transactions.incoming VALUES("
                        + record.t_id
                        + ","
                        + record.t_customer_id
                        + ","
                        + record.t_amount
                        + ")");

        Thread.sleep(1000);
    }
}
