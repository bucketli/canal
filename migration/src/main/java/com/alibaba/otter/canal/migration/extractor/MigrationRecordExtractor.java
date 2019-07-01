package com.alibaba.otter.canal.migration.extractor;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.migration.model.MigrationRecord;

import java.util.List;

/**
 * @author bucketli 2019-06-30 11:04
 * @since 1.1.3
 */
public interface MigrationRecordExtractor {

    /**
     * 扫描获取数据
     * @return
     * @throws CanalException
     */
    public List<MigrationRecord> extract() throws CanalException;
}
