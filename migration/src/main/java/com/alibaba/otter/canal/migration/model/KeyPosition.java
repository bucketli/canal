package com.alibaba.otter.canal.migration.model;

import com.alibaba.otter.canal.migration.process.ProgressStatus;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

/**
 * @author bucketli 2019-07-02 07:25
 * @since 1.1.3
 */
public class KeyPosition implements Serializable {

    private static final long    serialVersionUID = -7150026048661436002L;
    private Number               key;
    private List<ProgressStatus> progressHistory  = Lists.newArrayList();
    private ProgressStatus       current          = ProgressStatus.UNKNOW;

    public Number getKey() {
        return key;
    }

    public void setKey(Number key) {
        this.key = key;
    }

    public List<ProgressStatus> getProgressHistory() {
        return progressHistory;
    }

    public void setProgressHistory(List<ProgressStatus> progressHistory) {
        this.progressHistory = progressHistory;
    }

    public ProgressStatus getCurrent() {
        return current;
    }

    public void setCurrent(ProgressStatus current) {
        if (this.current != current && this.current != ProgressStatus.UNKNOW) {
            this.progressHistory.add(this.current);
        }

        this.current = current;
    }

    public boolean isInHistory(ProgressStatus status){
        return this.progressHistory.contains(status);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KeyPosition that = (KeyPosition) o;

        if (!key.equals(that.key)) return false;
        if (!progressHistory.equals(that.progressHistory)) return false;
        return current == that.current;
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + progressHistory.hashCode();
        result = 31 * result + current.hashCode();
        return result;
    }
}
