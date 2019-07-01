package com.alibaba.otter.canal.migration.model;

/**
 * @author bucketli 2019-07-02 07:18
 * @since 1.1.3
 */
public enum DBType {

    MySQL("com.mysql.jdbc.Driver");

    private String driver;

    DBType(String driver){
        this.driver = driver;
    }

    public String getDriver() {
        return this.driver;
    }

    public boolean isMySQL() {
        return this == DBType.MySQL;
    }
}
