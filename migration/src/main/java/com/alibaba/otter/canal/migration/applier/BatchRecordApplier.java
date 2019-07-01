package com.alibaba.otter.canal.migration.applier;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.migration.model.MigrationRecord;

import java.util.List;

/**
 * @author bucketli 2019-06-30 11:13
 * @since 1.1.3
 */
public class BatchRecordApplier extends AbstractCanalLifeCycle implements MigrationRecordApplier {

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void apply(List<MigrationRecord> records) throws CanalException {

    }

    @Override
    public void stop() {
        super.stop();
    }
}
