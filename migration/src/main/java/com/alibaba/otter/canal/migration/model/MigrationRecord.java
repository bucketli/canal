package com.alibaba.otter.canal.migration.model;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.migration.metadata.ColumnValue;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * @author bucketli 2019-06-30 11:06
 * @since 1.1.3
 */
public class MigrationRecord {

    private String            schemaName;
    private String            tableName;
    private List<ColumnValue> primaryKeys = Lists.newArrayList();
    private List<ColumnValue> columns     = Lists.newArrayList();

    private MigrationRecord(){
    }

    public MigrationRecord(String schemaName, String tableName, List<ColumnValue> primaryKeys,
                           List<ColumnValue> columns){
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.primaryKeys = primaryKeys;
        this.columns = columns;
    }

    private void addPrimaryKey(ColumnValue primaryKey) {
        if (getColumnByName(primaryKey.getColumn().getName(), true) != null) {
            throw new CanalException("duplicate column " + primaryKey.getColumn().getName());
        }

        primaryKeys.add(primaryKey);
    }

    private void addColumn(ColumnValue column) {
        if (getColumnByName(column.getColumn().getName(), true) != null) {
            throw new CanalException("duplicate column " + column.getColumn().getName());
        }

        columns.add(column);
    }

    /**
     * find ColumnValue by column name, include the ColumnValue in primaryKeys
     * 
     * @param columnName
     * @param returnNullIfNotExist
     * @return
     */
    private ColumnValue getColumnByName(String columnName, boolean returnNullIfNotExist) {
        for (ColumnValue c : columns) {
            if (c.getColumn().getName().equalsIgnoreCase(columnName)) {
                return c;
            }
        }

        for (ColumnValue pk : primaryKeys) {
            if (pk.getColumn().getName().equalsIgnoreCase(columnName)) {
                return pk;
            }
        }

        if (returnNullIfNotExist) {
            return null;
        } else {
            throw new CanalException(this.schemaName + "." + this.tableName + " not found column [" + columnName + "]");
        }
    }

    public MigrationRecord clone() {
        MigrationRecord r = new MigrationRecord();
        r.setSchemaName(schemaName);
        r.setTableName(tableName);
        for (ColumnValue c : primaryKeys) {
            r.addPrimaryKey(c.clone());
        }

        for (ColumnValue c : columns) {
            r.addColumn(c.clone());
        }

        return r;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<ColumnValue> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<ColumnValue> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public List<ColumnValue> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnValue> columns) {
        this.columns = columns;
    }

}
