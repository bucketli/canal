package com.alibaba.otter.canal.migration.extractor;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * @author bucketli 2019/7/8 2:52 PM
 * @since 1.1.3
 **/
public class Resumable extends AbstractCanalLifeCycle implements Runnable {

    private static final Logger                         logger  = LoggerFactory.getLogger(Resumable.class);
    private              FullRecordExtractor            fullRecordExtractor;
    private              JdbcTemplate                   jdbcTemplate;
    private              Object                         id      = 0L;
    private              BlockingQueue<MigrationRecord> queue;
    private volatile     boolean                        running = true;
    private              MigrationTable                 table;
    private              int                            crawSize;

    public Resumable(FullRecordExtractor fullRecordExtractor, BlockingQueue<MigrationRecord> queue,
                     DataSource dataSource, KeyPosition position, MigrationTable table, int crawSize){
        this.fullRecordExtractor = fullRecordExtractor;
        this.queue = queue;
        this.table = table;
        this.crawSize = crawSize;
        jdbcTemplate = new JdbcTemplate(dataSource);

        if (position != null) {
            if (position.getCurrent() == ProgressStatus.ETLING) {
                id = position.getKey();
            }

            if (id == null) {
                id = getMinId();
            }
        } else {
            id = getMinId();
        }

        logger.info(table.getFullName() + " start , position :" + id);
    }

    private Object getMinId() {
        Assert.notNull(jdbcTemplate);
        Assert.notNull(fullRecordExtractor.getMinimumKeySQL());

        Object min = jdbcTemplate.execute(fullRecordExtractor.getMinimumKeySQL(), (PreparedStatementCallback) ps -> {
            ResultSet rs = ps.executeQuery();
            Object re = null;
            while (rs.next()) {
                re = rs.getObject(1);
                break;
            }

            return re;
        });

        if (min != null) {
            if (min instanceof Number) {
                min = Long.valueOf(String.valueOf(min)) - 1;
            } else {
                min = "";
            }
        } else {
            if (min instanceof Number) {
                min = 0;
            } else {
                min = "";
            }
        }

        return min;
    }

    @Override
    public void run() {
        while (running) {
            jdbcTemplate.execute(fullRecordExtractor.getExtractSQL(), (PreparedStatementCallback) ps -> {
                ps.setObject(1, id);
                ps.setInt(2, crawSize);
                ps.setFetchSize(200);
                ResultSet rs = ps.executeQuery();

                List<MigrationRecord> result = Lists.newArrayListWithCapacity(crawSize);
                while (rs.next()) {
                    List<ColumnValue> cms = new ArrayList<>();
                    List<ColumnValue> pks = new ArrayList<>();

                    for (ColumnMeta col : table.getColumns()) {
                        ColumnValue cv = new ColumnValue(col, rs.getObject(col.getName()));
                        cms.add(cv);
                        if (col.isPrimaryKey()) {
                            pks.add(cv);

                            id = cv.getValue();
                        }
                    }

                    MigrationRecord r = new MigrationRecord(table.getSchema(), table.getName(), pks, cms);
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
