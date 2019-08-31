package com.alibaba.otter.canal.migration.applier;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import com.alibaba.otter.canal.migration.controller.MigrationUnit;
import com.alibaba.otter.canal.migration.utils.ExecutorTemplate;
import org.apache.commons.lang.StringUtils;
import org.slf4j.MDC;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.migration.metadata.ColumnValue;
import com.alibaba.otter.canal.migration.metadata.MigrationTable;
import com.alibaba.otter.canal.migration.model.MigrationRecord;

import javax.sql.DataSource;
import javax.xml.crypto.Data;

/**
 * @author bucketli 2019-06-30 11:13
 * @since 1.1.3
 */
public class BatchRecordApplier extends AbstractCanalLifeCycle implements MigrationRecordApplier {

    private static final String applySQLFormat = "insert into `{0}`({1}) values ({2})";
    private int                 threadSize     = Runtime.getRuntime().availableProcessors() * 2;
    private int                 batchSize      = 100;
    private ThreadPoolExecutor  executor;
    private String              executorName;
    private MigrationTable      table;
    private DataSource          dataSource;
    private String              applySQL;

    private BatchRecordApplier(){
    };

    public BatchRecordApplier(int threadSize, int batchSize, ThreadPoolExecutor executor, DataSource dataSource,
                              MigrationTable table){
        this.threadSize = threadSize;
        this.batchSize = batchSize;
        this.executor = executor;
        this.table = table;
        this.dataSource = dataSource;
        this.applySQL = this.getApplySQL();
    }

    @Override
    public void start() {
        super.start();

        executorName = this.getClass().getSimpleName() + "-" + table.getFullName();
        if (executor == null) {
            executor = new ThreadPoolExecutor(threadSize,
                threadSize,
                60,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(threadSize * 2),
                new NamedThreadFactory(executorName),
                new ThreadPoolExecutor.CallerRunsPolicy());
        }
    }

    protected String getApplySQL() {
        String cols = "`" + StringUtils.join(table.getColumnNames(), "`,`") + "`";
        String values = StringUtils.join(new Iterator() {

            private int size  = table.getColumnNames().size();
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index++ <= size;
            }

            @Override
            public Object next() {
                return "?";
            }
        }, ",");
        String sql = new MessageFormat(applySQLFormat).format(new Object[] { table.getName(), cols, values });
        return sql;
    }

    @Override
    public void apply(List<MigrationRecord> records) throws CanalException {
        if (records == null || records.size() == 0) {
            return;
        }

        if (records.size() > batchSize) {
            ExecutorTemplate template = new ExecutorTemplate(executor);
            int index = 0;
            int size = records.size();
            for (; index < size;) {
                int end = (index + batchSize > size) ? size : (index + batchSize);
                final List<MigrationRecord> subList = records.subList(index, end);
                template.submit(() -> {
                    String name = Thread.currentThread().getName();
                    try {
                        MDC.put(MigrationUnit.MDC_TABLE_KEY, table.getName());
                        Thread.currentThread().setName(executorName);
                        JdbcTemplate template1 = new JdbcTemplate(dataSource);
                        batchApply(template1, records);
                    } finally {
                        Thread.currentThread().setName(name);
                    }
                });
            }
        } else {
            JdbcTemplate template1 = new JdbcTemplate(dataSource);
            batchApply(template1, records);
        }
    }

    protected void batchApply(JdbcTemplate template, final List<MigrationRecord> records) {
        boolean serialApply = false;
        try {
            template.execute(applySQL, new PreparedStatementCallback() {

                @Override
                public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                    for (MigrationRecord r : records) {
                        int i = 1;
                        for (ColumnValue cv : r.getColumns()) {
                            ps.setObject(i, cv.getValue(), cv.getColumn().getType());
                            i++;
                        }

                        ps.addBatch();
                    }

                    ps.executeBatch();
                    return null;
                }
            });
        } catch (Exception e) {
            serialApply = true;
        }

        if (serialApply) {
            serialApply(template, records);
        }
    }

    protected void serialApply(JdbcTemplate template, final List<MigrationRecord> records) {
        template.execute(applySQL, new PreparedStatementCallback() {

            @Override
            public Object doInPreparedStatement(PreparedStatement ps) throws SQLException, DataAccessException {
                for (MigrationRecord r : records) {
                    int i = 1;
                    for (ColumnValue cv : r.getColumns()) {
                        ps.setObject(i, cv.getValue(), cv.getColumn().getType());
                        i++;
                    }

                    ps.execute();
                }

                return null;
            }
        });
    }

    @Override
    public void stop() {
        super.stop();

        if (executor != null) {
            this.executor.shutdownNow();
        }
    }
}
