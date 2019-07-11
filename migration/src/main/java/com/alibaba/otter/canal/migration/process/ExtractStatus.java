package com.alibaba.otter.canal.migration.process;

/**
 * @author bucketli 2019-07-04 07:44
 * @since 1.1.3
 */
public enum ExtractStatus {
    NORMAL, CATCH_UP, NO_UPDATE, TABLE_END;
}
