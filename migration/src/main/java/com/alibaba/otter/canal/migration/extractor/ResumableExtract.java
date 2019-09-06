package com.alibaba.otter.canal.migration.extractor;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.migration.metadata.ColumnMeta;
import com.alibaba.otter.canal.migration.metadata.ColumnValue;
import com.alibaba.otter.canal.migration.metadata.MigrationTable;
import com.alibaba.otter.canal.migration.model.KeyPosition;
import com.alibaba.otter.canal.migration.model.MigrationRecord;
import com.alibaba.otter.canal.migration.process.ExtractStatus;
import com.alibaba.otter.canal.migration.process.ProgressStatus;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * @author bucketli 2019/7/8 2:52 PM
 * @since 1.1.3
 **/
public class ResumableExtract extends AbstractCanalLifeCycle implements Runnable {

    private static final Logger                         logger              = LoggerFactory.getLogger(ResumableExtract.class);
    private static final String                         extractSQLFormat    = "select `{0}` from `{1}`.`{2}` where `{3}` > ? order by `{4}` limit ?";
    private static final String                         minimumKeySQLFormat = "select min(`{0}`) form `{1}`.`{2}`";
    private              String                         minimumKeySQL;
    private              String                         extractSQL;
    private              FullRecordExtractor            fullRecordExtractor;
    private              JdbcTemplate                   jdbcTemplate;
    private              Object                         id                  = 0L;
    private              BlockingQueue<MigrationRecord> queue;
    private volatile     boolean                        running             = true;
    private              MigrationTable                 table;
    private              int                            crawSize;

    public ResumableExtract(FullRecordExtractor fullRecordExtractor, BlockingQueue<MigrationRecord> queue,
                            DataSource dataSource, KeyPosition position, MigrationTable table, int crawSize) {
        this.fullRecordExtractor = fullRecordExtractor;
        this.queue = queue;
        this.table = table;
        this.crawSize = crawSize;
        jdbcTemplate = new JdbcTemplate(dataSource);

        setExtractSQL();
        setMinimumKeySQL();

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

    protected String setExtractSQL() {
        Preconditions.checkArgument(StringUtils.isNotBlank(extractSQL),
                "extractSQL already be set,sql is:" + extractSQL);
        String colStr = StringUtils.join(table.getColumnNames(), ",");
        extractSQL = new MessageFormat(extractSQLFormat).format(new Object[] { colStr, table.getSchema(),
                                                                               table.getName(),
                                                                               table.getPrimaryKeys().get(0),
                                                                               table.getPrimaryKeys().get(0) });
        return extractSQL;
    }

    protected String setMinimumKeySQL() {
        Preconditions.checkArgument(StringUtils.isNotBlank(minimumKeySQL),
                "minimumKeySQL already be set,sql is :" + minimumKeySQL);
        minimumKeySQL = new MessageFormat(minimumKeySQLFormat).format(new Object[] { table.getPrimaryKeys().get(0),
                                                                                     table.getSchema(),
                                                                                     table.getName() });
        return minimumKeySQL;
    }

    protected Object getMinId() {
        Assert.notNull(jdbcTemplate);
        Assert.notNull(this.minimumKeySQL);

        Object min = jdbcTemplate.execute(this.minimumKeySQL, (PreparedStatementCallback<Object>) ps -> {
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

    @Override public void run() {
        while (running) {
            jdbcTemplate.execute(this.extractSQL, (PreparedStatementCallback<Object>) ps -> {
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

                fullRecordExtractor.putResultToQueue(result);
                return null;
            });
        }
    }

    public BlockingQueue<MigrationRecord> getQueue() {
        return queue;
    }
}
