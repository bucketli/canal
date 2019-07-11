package com.alibaba.otter.canal.migration.process;

/**
 * @author bucketli 2019-07-02 07:12
 * @since 1.1.3
 */
public enum RunMode {
    /**
     * mark the binlog position
     */
    MARK,

    /**
     * binlog sync
     */
    SYNC,

    /**
     * ETL
     */
    ETL,

    /**
     * automatic run all phase
     */
    ALL;

    public boolean isMark() {
        return this == RunMode.MARK;
    }

    public boolean isSYNC(){
        return this == RunMode.SYNC;
    }

    public boolean isETL(){
        return this == RunMode.ETL;
    }

    public boolean isAll(){
        return this == RunMode.ALL;
    }
}
