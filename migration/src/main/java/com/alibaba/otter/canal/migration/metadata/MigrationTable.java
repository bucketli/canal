package com.alibaba.otter.canal.migration.metadata;

import com.google.common.collect.Lists;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.util.List;

/**
 * @author bucketli 2019-07-01 22:58
 * @since 1.1.3
 */
public class MigrationTable {

    private String           type;
    private String           schema;
    private String           name;
    private List<ColumnMeta> primaryKeys = Lists.newArrayList();
    /**
     * contain the primaryKeys;
     */
    private List<ColumnMeta> columns     = Lists.newArrayList();
    private List<String>     columnNames = Lists.newArrayList();

    public MigrationTable(String type, String schema, String name){
        this.type = type;
        this.schema = schema;
        this.name = name;
    }

    public MigrationTable(String type, String schema, String name, List<ColumnMeta> columns){
        this.type = type;
        this.schema = schema;
        this.name = name;
        this.columns = columns;

        for (ColumnMeta cm : columns) {
            columnNames.add(cm.getName());
            if (cm.isPrimaryKey()) {
                primaryKeys.add(cm);
            }
        }
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<ColumnMeta> getPrimaryKeys() {
        return primaryKeys;
    }

    public List<ColumnMeta> getColumns() {
        return columns;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumns(List<ColumnMeta> columns) {
        this.columns = columns;
        primaryKeys = Lists.newArrayList();
        columnNames= Lists.newArrayList();
        for (ColumnMeta cm : columns) {
            columnNames.add(cm.getName());
            if (cm.isPrimaryKey()) {
                primaryKeys.add(cm);
            }
        }
    }

    /**
     * get table full name
     * 
     * @return
     */
    public String getFullName() {
        return this.schema + "." + this.name;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MigrationTable that = (MigrationTable) o;

        if (!type.equals(that.type)) return false;
        if (!schema.equals(that.schema)) return false;
        if (!name.equals(that.name)) return false;
        return columns.equals(that.columns);
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + schema.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + columns.hashCode();
        return result;
    }
}
