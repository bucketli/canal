package com.alibaba.otter.canal.migration.mock;

import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author bucketli 2019/9/4 5:36 PM
 * @since 1.1.3
 **/
public class MockDataSourceTest {
    @Test
    public void testStatement(){
        try {
            MockDataSource ds = new MockDataSource();
            Connection c= ds.getConnection();
            Statement s=c.createStatement();
            ResultSet rs=s.executeQuery("select min(id) from t");
            while(rs.next()){
                Assert.assertEquals(10,rs.getInt(1));
            }
        }catch(SQLException e){
            Assert.fail();
        }
    }
}
