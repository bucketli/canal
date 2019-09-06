package com.alibaba.otter.canal.migration.extractor;

import com.alibaba.otter.canal.migration.metadata.ColumnMeta;
import com.alibaba.otter.canal.migration.metadata.MigrationTable;
import com.alibaba.otter.canal.migration.model.KeyPosition;
import com.alibaba.otter.canal.migration.mock.MockDataSource;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Tainted;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author bucketli 2019/9/4 6:35 PM
 * @since 1.1.3
 **/
public class ResumableExtractUnitTest {

    private ResumableExtract e;

    @BeforeClass public void init() {
        KeyPosition position = new KeyPosition();
        List<ColumnMeta> columnMetas = new ArrayList<>();
        ColumnMeta cm = new ColumnMeta("id", Types.INTEGER, true);
        ColumnMeta cm2 = new ColumnMeta("name", Types.VARCHAR, false);
        ColumnMeta cm3 = new ColumnMeta("age", Types.DATE, false);
        columnMetas.add(cm);
        columnMetas.add(cm2);
        columnMetas.add(cm3);
        MigrationTable table = new MigrationTable("TABLE", "TEST_DB", "test_Table", columnMetas);
        e = new ResumableExtract(null, new LinkedBlockingDeque<>(10), new MockDataSource(), position, table, 1000);
    }

    @Test public void testSetMinimumKeySQL() {
        String minimumSQL = e.setMinimumKeySQL();
        Assert.assertEquals("", minimumSQL);
    }

    @Test public void testGetMinId() {
        Object id = e.getMinId();
        Assert.assertEquals("", (int) id);
    }

    @Test public void testSetExtractSQL() {
        String extractSQL = e.setExtractSQL();
        Assert.assertEquals("", extractSQL);
    }

    @Test public void testRun() {
        e.run();
        BlockingQueue queue = e.getQueue();
        Assert.assertEquals(1, queue.size());
    }
}
