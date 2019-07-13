package com.alibaba.otter.canal.migration.extractor;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.migration.model.KeyPosition;
import com.alibaba.otter.canal.migration.model.MigrationRecord;
import com.alibaba.otter.canal.migration.process.ExtractStatus;

import java.util.List;

/**
 * @author bucketli 2019-06-30 11:04
 * @since 1.1.3
 */
public interface MigrationRecordExtractor extends CanalLifeCycle {

    /**
     * extract data from DataSource
     *
     * @return
     * @throws CanalException
     */
    public List<MigrationRecord> extract() throws CanalException;

    /**
     * return the success info
     *
     * @param records
     * @return
     * @throws CanalException
     */
    public KeyPosition ack(List<MigrationRecord> records) throws CanalException;

    /**
     * return the extractor status
     *
     * @return
     */
    public ExtractStatus status();
}
