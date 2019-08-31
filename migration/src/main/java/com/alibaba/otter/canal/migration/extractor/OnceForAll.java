package com.alibaba.otter.canal.migration.extractor;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.migration.metadata.ColumnMeta;
import com.alibaba.otter.canal.migration.metadata.ColumnValue;
import com.alibaba.otter.canal.migration.metadata.MigrationTable;
import com.alibaba.otter.canal.migration.model.KeyPosition;
import com.alibaba.otter.canal.migration.model.MigrationRecord;
import com.alibaba.otter.canal.migration.process.ExtractStatus;
import com.alibaba.otter.canal.migration.process.ProgressStatus;
import com.google.common.collect.Lists;

/**
 * @author bucketli 2019/7/8 2:52 PM
 * @since 1.1.3
 **/
public class OnceForAll extends AbstractCanalLifeCycle implements Runnable {

    private static final Logger                         logger  = LoggerFactory.getLogger(OnceForAll.class);
    private              FullRecordExtractor            fullRecordExtractor;
    private              JdbcTemplate                   jdbcTemplate;
    private              Object                         id      = 0L;
    private              BlockingQueue<MigrationRecord> queue;
    private volatile     boolean                        running = true;
    private              MigrationTable                 table;
    private              int                            crawSize;

    public OnceForAll(FullRecordExtractor fullRecordExtractor, BlockingQueue<MigrationRecord> queue,
                      DataSource dataSource, MigrationTable table){
        this.fullRecordExtractor = fullRecordExtractor;
        this.queue = queue;
        this.table = table;
        jdbcTemplate = new JdbcTemplate(dataSource);

        logger.info(table.getFullName() + " start , position :" + id);
    }

    @Override
    public void run() {
        while (running) {
            jdbcTemplate.execute(fullRecordExtractor.getExtractSQL(), (PreparedStatementCallback) ps -> {
                ps.setFetchSize(200);
                ResultSet rs = ps.executeQuery();

                List<MigrationRecord> result = Lists.newArrayListWithCapacity(crawSize);
                while (rs.next()) {
                    List<ColumnValue> cms = new ArrayList<>();

                    for (ColumnMeta col : table.getColumns()) {
                        ColumnValue cv = new ColumnValue(col, rs.getObject(col.getName()));
                        cms.add(cv);
                    }

                    MigrationRecord r = new MigrationRecord(table.getSchema(), table.getName(), new ArrayList<>(), cms);
                    result.add(r);
                }

                if (result.size() < 1) {
                    fullRecordExtractor.setStatus(ExtractStatus.TABLE_END);
                    running = false;
                }

                for (MigrationRecord r : result) {
                    try {
                        queue.put(r);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new CanalException(e);
                    }
                }

                return null;
            });
        }
    }
}
