package com.alibaba.otter.canal.migration.mock;

import java.sql.*;

import static org.mockito.Mockito.*;

/**
 * @author bucketli 2019-09-08 15:31
 * @since 1.1.3
 */
public class DefaultMockDataSource extends MockDataSource {

    @Override
    public Connection mockConnection() throws SQLException {
        final Connection mockConnection = mock(Connection.class);
        final Statement statement = mock(Statement.class);
        final PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        final ResultSet mockResultSet = mock(ResultSet.class);

        return mockConnectionCommon(mockConnection, statement, mockPreparedStatement, mockResultSet);
    }
}
