package com.alibaba.otter.canal.migration.applier;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.migration.model.MigrationRecord;

import java.util.List;

/**
 * @author bucketli 2019-06-30 11:10
 * @since 1.1.3
 */
public interface MigrationRecordApplier extends CanalLifeCycle {

    /**
     * 将一批数据写入对端
     * @param records
     * @throws CanalException
     */
    public void apply(List<MigrationRecord> records) throws CanalException;
}
