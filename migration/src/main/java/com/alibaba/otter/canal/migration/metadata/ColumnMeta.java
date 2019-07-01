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

    private String name;
    private int    type;

    public ColumnMeta(String columnName, int columnType){
        this.name = StringUtils.upperCase(columnName);
        this.type = columnType;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnMeta that = (ColumnMeta) o;

        if (type != that.type) return false;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type;
        return result;
    }
}
