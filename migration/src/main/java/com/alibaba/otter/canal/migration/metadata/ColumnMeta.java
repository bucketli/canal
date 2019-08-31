package com.alibaba.otter.canal.migration.metadata;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.util.Objects;

/**
 * @author bucketli 2019-07-01 22:36
 * @since 1.1.3
 */
public class ColumnMeta {

    private String  name;
    private int     type;
    private boolean isPrimaryKey = false;

    public ColumnMeta(String columnName, int columnType, boolean isPrimaryKey){
        this.name = StringUtils.upperCase(columnName);
        this.type = columnType;
        this.isPrimaryKey = isPrimaryKey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }
}
