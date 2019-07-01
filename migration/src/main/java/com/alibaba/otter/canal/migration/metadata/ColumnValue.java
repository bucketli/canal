package com.alibaba.otter.canal.migration.metadata;

/**
 * @author bucketli 2019-07-01 22:47
 * @since 1.1.3
 */
public class ColumnValue {

    private ColumnMeta column;
    private Object     value;

    private ColumnValue(){
    }

    public ColumnValue(ColumnMeta column, Object value){
        this.value = value;
        this.column = column;
    }

    public ColumnMeta getColumn() {
        return column;
    }

    public void setColumn(ColumnMeta column) {
        this.column = column;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public ColumnValue clone() {
        return new ColumnValue(this.column, this.value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnValue that = (ColumnValue) o;

        if (!column.equals(that.column)) return false;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        int result = column.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
