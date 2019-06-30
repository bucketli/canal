package com.alibaba.otter.canal.migration.extractor;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.migration.model.MigrationRecord;

import java.util.List;

/**
 * @author bucketli 2019-06-30 11:01
 * @since 1.1.3
 */
public class KeyRecordExtractor extends AbstractCanalLifeCycle implements MigrationRecordExtractor {

    @Override
    public void start() {
        super.start();
    }

    @Override
    public List<MigrationRecord> extract() throws CanalException {
        return null;
    }

    @Override
    public void stop() {
        super.stop();
    }
}
