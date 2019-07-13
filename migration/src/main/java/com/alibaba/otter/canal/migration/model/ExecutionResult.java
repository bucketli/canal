package com.alibaba.otter.canal.migration.model;

/**
 * @author bucketli 2019/7/12 10:18 AM
 * @since 1.1.3
 **/
public class ExecutionResult {

    private KeyPosition position;

    public KeyPosition getPosition() {
        return position;
    }

    public void setPosition(KeyPosition position) {
        this.position = position;
    }
}
