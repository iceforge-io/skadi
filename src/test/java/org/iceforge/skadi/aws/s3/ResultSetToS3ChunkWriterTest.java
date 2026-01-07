package org.iceforge.skadi.aws.s3;

import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;


import static org.junit.jupiter.api.Assertions.assertEquals;

class ResultSetToS3ChunkWriterTest {

    private static Connection connection;
    private ResultSetToS3ChunkWriter writer;
    private AwsSdkS3AccessLayer mockS3;

    @BeforeAll
    static void setupDatabase() throws Exception {
        connection = DriverManager.getConnection("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        connection.createStatement().execute("CREATE TABLE test_table (id INT PRIMARY KEY, name VARCHAR(255))");
        connection.createStatement().execute("INSERT INTO test_table (id, name) VALUES (1, 'Alice'), (2, 'Bob')");
    }

    @AfterAll
    static void tearDownDatabase() throws Exception {
        connection.close();
    }

    @BeforeEach
    void setup() {
        mockS3 = Mockito.mock(AwsSdkS3AccessLayer.class);
        writer = new ResultSetToS3ChunkWriter(mockS3);
    }

    @Test
    void testWrite() throws Exception {
        ResultSetToS3ChunkWriter.S3WritePlan plan = new ResultSetToS3ChunkWriter.S3WritePlan(
                "test-bucket", "test-prefix", "test-run"
        );
        ResultSetToS3ChunkWriter.StreamOptions options = ResultSetToS3ChunkWriter.StreamOptions.defaults();

        ResultSetToS3ChunkWriter.S3ResultSetRef result = writer.write(
                connection, "SELECT * FROM test_table", plan, options
        );

        Mockito.verify(mockS3, Mockito.atLeastOnce()).putBytes(
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()
        );

        assertEquals(2, result.rowCount());
        assertEquals(1, result.chunkCount());
    }
}