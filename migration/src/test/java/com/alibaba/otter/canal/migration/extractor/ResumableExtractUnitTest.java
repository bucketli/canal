package com.alibaba.otter.canal.migration.extractor;

import com.alibaba.otter.canal.migration.metadata.ColumnMeta;
import com.alibaba.otter.canal.migration.metadata.MigrationTable;
import com.alibaba.otter.canal.migration.mock.DefaultMockDataSource;
import com.alibaba.otter.canal.migration.model.KeyPosition;
import com.alibaba.otter.canal.migration.mock.MockDataSource;
import com.alibaba.otter.canal.migration.model.MigrationRecord;
import com.alibaba.otter.canal.migration.process.ExtractStatus;
import com.alibaba.otter.canal.migration.process.RunMode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Tainted;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

/**
 * @author bucketli 2019/9/4 6:35 PM
 * @since 1.1.3
 **/
public class ResumableExtractUnitTest {

    private static KeyPosition      position;
    private static MigrationTable   table;
    private static ResumableExtract e;

    @BeforeClass
    public static void init() {
        KeyPosition position = new KeyPosition();
        List<ColumnMeta> columnMetas = new ArrayList<>();
        ColumnMeta cm = new ColumnMeta("id", Types.INTEGER, true);
        ColumnMeta cm2 = new ColumnMeta("name", Types.VARCHAR, false);
        ColumnMeta cm3 = new ColumnMeta("gmt_create", Types.DATE, false);
        columnMetas.add(cm);
        columnMetas.add(cm2);
        columnMetas.add(cm3);
        table = new MigrationTable("TABLE", "TEST_DB", "test_table", columnMetas);
        FullRecordExtractor fullRecordExtractor = new FullRecordExtractor(table,
            new DefaultMockDataSource(),
            position,
            1000,
            RunMode.ETL);
        e = new ResumableExtract(fullRecordExtractor);
    }

    @Test
    public void testSetMinimumKeySQL() {
        String minimumSQL = e.setMinimumKeySQL();
        Assert.assertEquals("select min(`id`) form `TEST_DB`.`test_table`", minimumSQL);
    }

    @Test
    public void testGetMinIdNumber() throws SQLException {
        FullRecordExtractor fullRecordExtractor = new FullRecordExtractor(table, new MockDataSource() {

            @Override
            public Connection mockConnection() throws SQLException {
                Connection mockConnection = mock(Connection.class);
                Statement mockStatement = mock(Statement.class);
                PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
                ResultSet mockMinIdResultSet = mock(ResultSet.class);

                super.mockConnectionCommon(mockConnection, mockStatement, mockPreparedStatement, mockMinIdResultSet);
                super.mockPrepareStatementCommon(mockPreparedStatement);
                super.mockResultSetCommon(mockMinIdResultSet, 2);
                when(mockMinIdResultSet.getObject(1)).thenReturn(20L);
                return mockConnection;
            }
        }, position, 1000, RunMode.ETL);
        ResumableExtract e = new ResumableExtract(fullRecordExtractor);
        e.setMinimumKeySQL();

        Object id = e.getMinId();
        Assert.assertEquals(19, (long) id);
    }

    @Test
    public void testGetMinIdString() throws SQLException {
        FullRecordExtractor fullRecordExtractor = new FullRecordExtractor(table, new MockDataSource() {

            @Override
            public Connection mockConnection() throws SQLException {
                Connection mockConnection = mock(Connection.class);
                Statement mockStatement = mock(Statement.class);
                PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
                ResultSet mockMinIdResultSet = mock(ResultSet.class);

                super.mockConnectionCommon(mockConnection, mockStatement, mockPreparedStatement, mockMinIdResultSet);
                super.mockPrepareStatementCommon(mockPreparedStatement);
                super.mockResultSetCommon(mockMinIdResultSet, 2);
                when(mockMinIdResultSet.getObject(1)).thenReturn("abc");
                return mockConnection;
            }
        }, position, 1000, RunMode.ETL);
        ResumableExtract e = new ResumableExtract(fullRecordExtractor);
        e.setMinimumKeySQL();

        Object id = e.getMinId();
        Assert.assertEquals("", id);
    }

    @Test
    public void testSetExtractSQL() {
        String extractSQL = e.setExtractSQL();
        Assert.assertEquals(
            "select `id,name,gmt_create` from `TEST_DB`.`test_table` where `id` > ? order by `id` limit ?",
            extractSQL);
    }

    @Test
    public void testRun() {
        FullRecordExtractor fullRecordExtractor = new FullRecordExtractor(table, new MockDataSource() {

            @Override
            public Connection mockConnection() throws SQLException {
                Connection mockConnection = mock(Connection.class);
                Statement mockStatement = mock(Statement.class);
                PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
                ResultSet mockMinIdResultSet = mock(ResultSet.class);

                super.mockConnectionCommon(mockConnection, mockStatement, mockPreparedStatement, mockMinIdResultSet);
                super.mockPrepareStatementCommon(mockPreparedStatement);
                super.mockResultSetCommon(mockMinIdResultSet, 3);

                doAnswer((invocationOnMock) -> {
                    return null;
                }).doNothing().when(mockPreparedStatement).setFetchSize(anyInt());
                when(mockMinIdResultSet.getObject("id")).thenReturn(99);
                when(mockMinIdResultSet.getObject("name")).thenReturn("bucketli");
                when(mockMinIdResultSet.getObject("gmt_create")).thenReturn("2019-12-12 12:12:12");

                return mockConnection;
            }
        }, position, 1000, RunMode.ETL);
        ResumableExtract ex = new ResumableExtract(fullRecordExtractor);
        ex.setExtractSQL();
        ex.run();
        BlockingQueue queue = fullRecordExtractor.getQueue();
        Assert.assertEquals(ExtractStatus.TABLE_END, fullRecordExtractor.status());
        Assert.assertEquals(ex.isExtracting(), false);
        Assert.assertEquals(3, queue.size());
        Assert.assertEquals(99, ((MigrationRecord) queue.poll()).getPrimaryKeys().get(0).getValue());
        Assert.assertEquals("bucketli", ((MigrationRecord) queue.poll()).getColumns().get(1).getValue());
        Assert.assertEquals("2019-12-12 12:12:12", ((MigrationRecord) queue.poll()).getColumns().get(2).getValue());
    }
}
