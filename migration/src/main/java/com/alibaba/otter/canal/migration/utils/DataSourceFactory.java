package com.alibaba.otter.canal.migration.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.migration.model.DBType;
import com.alibaba.otter.canal.migration.model.DataSourceConfig;
import com.google.common.base.Function;
import com.google.common.collect.MigrateMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.util.Map;
import java.util.Properties;

/**
 * @author bucketli 2019/7/30 3:40 PM
 * @since 1.1.3
 **/
public class DataSourceFactory extends AbstractCanalLifeCycle {

    private static final Logger               logger      = LoggerFactory.getLogger(DataSourceFactory.class);
    private final int                         maxWait     = 5 * 1000;
    private final int                         minIdle     = 0;
    private final int                         initialSize = 0;
    private final int                         maxActive   = 32;

    private Map<DataSourceConfig, DataSource> dataSources;

    @Override
    public void start() {
        super.start();

        dataSources = MigrateMap.makeComputingMap((dataSourceConfig) -> {
            return createDataSource(dataSourceConfig.getUrl(),
                dataSourceConfig.getUsername(),
                dataSourceConfig.getPassword(),
                dataSourceConfig.getType(),
                dataSourceConfig.getProperties());
        });
    }

    @Override
    public void stop() {
        super.stop();
        for (DataSource dataSource : dataSources.values()) {
            DruidDataSource druidDataSource = (DruidDataSource) dataSource;
            druidDataSource.close();
        }

        dataSources.clear();
    }

    public DataSource getDataSource(DataSourceConfig config) {
        return dataSources.get(config);
    }

    private DataSource createDataSource(String url, String userName, String password, DBType dbType,
                                        Properties properties) {
        try {
            int maxActive = Integer.valueOf(properties.getProperty("maxActive", String.valueOf(this.maxActive)));
            if (maxActive < 0) {
                maxActive = 200;
            }
            DruidDataSource dataSource = new DruidDataSource();
            dataSource.setUrl(url);
            dataSource.setUsername(userName);
            dataSource.setPassword(password);
            dataSource.setUseUnfairLock(true);
            dataSource.setNotFullTimeoutRetryCount(2);
            dataSource.setInitialSize(initialSize);
            dataSource.setMinIdle(minIdle);
            dataSource.setMaxActive(maxActive);
            dataSource.setDriverClassName(dbType.getDriver());

            if (properties != null && properties.size() > 0) {
                properties.entrySet().forEach(entry -> {
                    dataSource.addConnectionProperty(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
                });
            }

            if (dbType.isMySQL()) {
                dataSource.addConnectionProperty("useServerPrepStmts", "false");
                dataSource.addConnectionProperty("rewriteBatchedStatements", "true");
                dataSource.addConnectionProperty("allowMultiQueries", "true");
                dataSource.addConnectionProperty("readOnlyPropagatesToServer", "false");
                dataSource.setValidationQuery("select 1");
                dataSource.setExceptionSorter("com.alibaba.druid.pool.vendor.MySqlExceptionSorter");
                dataSource
                    .setValidConnectionCheckerClassName("com.alibaba.druid.pool.vendor.MySqlValidConnectionChecker");
            } else {
                logger.error("unknow datasource type.");
            }

            return dataSource;
        } catch (Throwable e) {
            throw new CanalException("create datasource error! url:" + url + ",user:" + userName, e);
        }
    }
}
