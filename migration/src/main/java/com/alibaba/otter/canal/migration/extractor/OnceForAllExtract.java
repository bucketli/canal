package com.alibaba.otter.canal.migration.extractor;

import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import javax.sql.DataSource;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.migration.metadata.ColumnMeta;
import com.alibaba.otter.canal.migration.metadata.ColumnValue;
import com.alibaba.otter.canal.migration.metadata.MigrationTable;
import com.alibaba.otter.canal.migration.model.MigrationRecord;
import com.alibaba.otter.canal.migration.process.ExtractStatus;
import com.google.common.collect.Lists;

/**
 * @author bucketli 2019/7/8 2:52 PM
 * @since 1.1.3
 **/
public class OnceForAllExtract extends AbstractCanalLifeCycle implements Runnable {

    private static final Logger            logger           = LoggerFactory.getLogger(OnceForAllExtract.class);

    private static final String            extractSQLFormat = "select `{0}` from `{1}`.`{2}`";
    private String                         extractSQL;
    private FullRecordExtractor            fullRecordExtractor;
    private JdbcTemplate                   jdbcTemplate;
    private Object                         id               = 0L;
    private BlockingQueue<MigrationRecord> queue;
    private volatile boolean               running          = true;
    private MigrationTable                 table;
    private int                            crawSize;

    public OnceForAllExtract(FullRecordExtractor fullRecordExtractor, BlockingQueue<MigrationRecord> queue,
                             DataSource dataSource, MigrationTable table, int crawSize){
        this.fullRecordExtractor = fullRecordExtractor;
        this.queue = queue;
        this.table = table;
        this.crawSize = crawSize;
        jdbcTemplate = new JdbcTemplate(dataSource);

        setExtractSQL();

        logger.info(table.getFullName() + " start , position :" + id);
    }

    protected void setExtractSQL() {
        Preconditions.checkArgument(StringUtils.isNotBlank(extractSQL),
            "extractSQL already be set,sql is:" + extractSQL);
        String colStr = StringUtils.join(table.getColumnNames(), ",");
        extractSQL = new MessageFormat(extractSQLFormat)
            .format(new Object[] { colStr, table.getSchema(), table.getName(), table.getPrimaryKeys().get(0),
                                   table.getPrimaryKeys().get(0) });
    }

    @Override
    public void run() {
        while (running) {
            jdbcTemplate.execute(this.extractSQL, (PreparedStatementCallback<Object>) ps -> {
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

                    if(result.size() >= crawSize){
                        fullRecordExtractor.putResultToQueue(result);
                        result = Lists.newArrayListWithCapacity(crawSize);
                    }
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
}
