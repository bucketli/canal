package com.alibaba.otter.canal.migration.mock;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author bucketli 2019/9/4 5:36 PM
 * @since 1.1.3
 **/
public class MockDataSourceTest {

    @Test
    public void testStatement() {
        try {
            MockDataSource ds = new MockDataSource() {

                @Override
                public Connection mockConnection() throws SQLException {
                    Connection mockConnection = mock(Connection.class);
                    Statement mockStatement = mock(Statement.class);
                    PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
                    ResultSet mockResultSet = mock(ResultSet.class);

                    super.mockConnectionCommon(mockConnection, mockStatement, mockPreparedStatement, mockResultSet);
                    super.mockPrepareStatementCommon(mockPreparedStatement);
                    super.mockResultSetCommon(mockResultSet, 10);
                    when(mockResultSet.getInt(1)).thenReturn(20);
                    return mockConnection;
                }
            };
            Connection c = ds.getConnection();
            Statement s = c.createStatement();
            ResultSet rs = s.executeQuery("select min(id) from t");
            while (rs.next()) {
                Assert.assertEquals(20, rs.getInt(1));
            }
        } catch (SQLException e) {
            Assert.fail();
        }
    }
}
