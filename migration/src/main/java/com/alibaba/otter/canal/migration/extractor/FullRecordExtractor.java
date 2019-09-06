package com.alibaba.otter.canal.migration.extractor;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.migration.metadata.ColumnValue;
import com.alibaba.otter.canal.migration.metadata.MigrationTable;
import com.alibaba.otter.canal.migration.model.KeyPosition;
import com.alibaba.otter.canal.migration.model.MigrationRecord;
import com.alibaba.otter.canal.migration.process.ExtractStatus;
import com.alibaba.otter.canal.migration.process.ProgressStatus;
import com.alibaba.otter.canal.migration.process.RunMode;
import com.google.common.collect.Lists;

import javax.sql.DataSource;

/**
 * @author bucketli 2019-06-30 11:01
 * @since 1.1.3
 */
public class FullRecordExtractor extends AbstractCanalLifeCycle implements MigrationRecordExtractor {

    private final MigrationTable                 table;
    private final int                            crawSize;
    private final DataSource                     dataSource;
    private final KeyPosition                    position;

    private volatile ExtractStatus               status          = ExtractStatus.NORMAL;
    private Thread                               extractorThread = null;
    private LinkedBlockingQueue<MigrationRecord> queue;

    public FullRecordExtractor(MigrationTable table, DataSource dataSource, KeyPosition position, int crawSize,
                               RunMode runMode){
        this.table = table;
        this.dataSource = dataSource;
        this.position = position;
        this.crawSize = crawSize;
    }

    @Override
    public void start() {
        super.start();

        queue = new LinkedBlockingQueue<>(crawSize * 2);
        if (table.getPrimaryKeys().size() == 1) {
            extractorThread = new NamedThreadFactory(this.getClass().getSimpleName() + "-" + table.getFullName())
                .newThread(new ResumableExtract(this, queue, dataSource, position, table, crawSize));
        } else {
            extractorThread = new NamedThreadFactory(this.getClass().getSimpleName() + "-" + table.getFullName())
                .newThread(new OnceForAllExtract(this, queue, dataSource, table, crawSize));
        }

        extractorThread.start();
    }

    @Override
    public List<MigrationRecord> extract() throws CanalException {
        List<MigrationRecord> rs = Lists.newArrayListWithCapacity(this.crawSize);
        for (int i = 0; i < crawSize; i++) {
            MigrationRecord r = queue.poll();
            if (r != null) {
                rs.add(r);
            } else if (status() == ExtractStatus.TABLE_END) {
                // check again
                MigrationRecord r1 = queue.poll();
                if (r1 != null) {
                    rs.add(r1);
                } else {
                    // no record actually,break the loop
                    break;
                }
            } else {
                // poll no record , reduce the iterator ,try again
                i--;
                continue;
            }
        }

        return rs;
    }

    public void putResultToQueue(List<MigrationRecord> result){
        for (MigrationRecord r : result) {
            try {
                queue.put(r);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CanalException(e);
            }
        }
    }

    @Override
    public KeyPosition ack(List<MigrationRecord> records) throws CanalException {
        if (records != null && records.size() != 0) {
            MigrationRecord r = records.get(records.size() - 1);

            // set current progress status
            position.setCurrent(ProgressStatus.ETLING);

            // set last key
            List<ColumnValue> pks = r.getPrimaryKeys();
            if (pks != null && pks.size() != 0) {
                Object key = pks.get(0).getValue();
                if (key instanceof Number) {
                    position.setKey((Number) key);
                }
            }

            return position;
        } else {
            return null;
        }
    }

    @Override
    public ExtractStatus status() {
        return status;
    }

    public void setStatus(ExtractStatus status) {
        this.status = status;
    }

    @Override
    public void stop() {
        super.stop();
        extractorThread.interrupt();
        try {
            extractorThread.join(2 * 1000);
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
