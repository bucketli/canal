package com.alibaba.otter.canal.migration.process;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.common.CanalLifeCycle;
import com.alibaba.otter.canal.migration.model.KeyPosition;

/**
 * @author bucketli 2019/7/8 1:35 PM
 * @since 1.1.3
 **/
public interface RecordPositioner {

    public KeyPosition getLast();

    public void persist(KeyPosition position) throws CanalException;

}
