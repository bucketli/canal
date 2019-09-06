package com.alibaba.otter.canal.migration.controller;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import com.alibaba.otter.canal.migration.applier.BatchRecordApplier;
import com.alibaba.otter.canal.migration.applier.MigrationRecordApplier;
import com.alibaba.otter.canal.migration.extractor.FullRecordExtractor;
import com.alibaba.otter.canal.migration.extractor.MigrationRecordExtractor;
import com.alibaba.otter.canal.migration.metadata.MigrationTable;
import com.alibaba.otter.canal.migration.model.DBType;
import com.alibaba.otter.canal.migration.model.DataSourceConfig;
import com.alibaba.otter.canal.migration.process.FileBasedRecordPositioner;
import com.alibaba.otter.canal.migration.process.MigrationConstants;
import com.alibaba.otter.canal.migration.process.RecordPositioner;
import com.alibaba.otter.canal.migration.process.RunMode;
import com.alibaba.otter.canal.migration.utils.DataSourceFactory;
import com.alibaba.otter.canal.migration.utils.LikeUtil;
import com.alibaba.otter.canal.migration.utils.TableMetaGenerator;
import com.google.common.collect.Lists;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

/**
 * @author bucketli 2019-06-30 10:46
 * @since 1.1.3
 */
public class MigrationController extends AbstractCanalLifeCycle {

    private static final Logger      logger            = LoggerFactory.getLogger(MigrationController.class);

    private final DataSourceFactory  dataSourceFactory = new DataSourceFactory();
    private RunMode                  runMode;
    // private DBType sourceDBType = DBType.MySQL;
    // private DBType targetDBType = DBType.MySQL;
    private DataSource               sourceDataSource;
    private DataSource               targetDataSource;
    private TableController          tableController;
    private List<MigrationUnit>      migrationUnits    = Lists.newArrayList();

    // private ThreadPoolExecutor extractorExecutor = null;
    private ThreadPoolExecutor       applierExecutor   = null;
    private ScheduledExecutorService scheduler;

    private Configuration            config;

    public MigrationController(Configuration config){
        this.config = config;
    }

    @Override
    public void start() {
        super.start();

        if (!dataSourceFactory.isStart()) {
            dataSourceFactory.start();
        }

        String r = config.getString(MigrationConstants.RUN_MODE);
        if (StringUtils.isBlank(r)) {
            throw new CanalException(MigrationConstants.RUN_MODE + " can not be null.");
        }

        this.runMode = RunMode.valueOf(r);
        // this.sourceDBType = DBType
        // .valueOf(StringUtils.upperCase(config.getString(MigrationConstants.SOURCE_DATABASE_TYPE)));
        // this.targetDBType = DBType
        // .valueOf(StringUtils.upperCase(config.getString(MigrationConstants.TARGET_DATABASE_TYPE)));

        sourceDataSource = initDataSource(MigrationConstants.SOURCE_DATABASE_TYPE);
        targetDataSource = initDataSource(MigrationConstants.TARGET_DATABASE_TYPE);

        int dop = config.getInt(MigrationConstants.ETL_CONCURRENT_SIZE);
        List<MigrationTable> tables = initTables();

        tableController = new TableController(tables.size(), dop);

        // int extractorDop =
        // config.getInt(MigrationConstants.ETL_EXTRACTOR_CONCURRENT_SIZE, 2);
        // extractorExecutor = new ThreadPoolExecutor(extractorDop,
        // extractorDop,
        // 60,
        // TimeUnit.SECONDS,
        // new ArrayBlockingQueue<>(extractorDop * 2),
        // new NamedThreadFactory("ETL-Data-Extractor"),
        // new ThreadPoolExecutor.CallerRunsPolicy());

        int applierDop = config.getInt(MigrationConstants.ETL_APPLIER_CONCURRENT_SIZE, 4);
        applierExecutor = new ThreadPoolExecutor(applierDop,
            applierDop,
            60,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(applierDop * 2),
            new NamedThreadFactory("ETL-Data-Applier"),
            new ThreadPoolExecutor.CallerRunsPolicy());

        int crawSize = config.getInt(MigrationConstants.EXTRACTOR_CRAW_SIZE, 1000);
        int batchSize = config.getInt(MigrationConstants.APPLIER_BATCH_SIZE, 100);

        for (MigrationTable table : tables) {
            RecordPositioner positioner = choosePositioner(table);
            MigrationRecordExtractor extractor = chooseExtractor(table, runMode, positioner, crawSize);
            MigrationRecordApplier applier = chooseApplier(table, runMode, applierDop, batchSize);

            MigrationUnit unit = new MigrationUnit(extractor, applier, positioner, tableController, table);
            migrationUnits.add(unit);
        }

        scheduler = Executors.newScheduledThreadPool(2);
        scheduler.execute(() -> {
            while (true) {
                try {
                    MigrationUnit unit = tableController.takeDone();
                    if (unit.isStart()) {
                        unit.stop();
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Throwable e) {
                    logger.error("stop error.", e);
                }
            }
        });

        for (MigrationUnit unit : migrationUnits) {
            unit.start();
        }
    }

    protected RecordPositioner choosePositioner(MigrationTable table) {
        FileBasedRecordPositioner positioner = new FileBasedRecordPositioner();
        positioner.setPositionDir(new File(MigrationConstants.POSITION_FILE_DIR));
        positioner.setPositionFileName(table.getSchema() + "_" + table.getName() + ".dat");
        return positioner;
    }

    protected MigrationRecordExtractor chooseExtractor(MigrationTable table, RunMode runMode,
                                                       RecordPositioner positioner, int crawSize) {
        // TODO consider other run mode
        if (runMode == RunMode.ETL || runMode == RunMode.CHECK) {
            // TODO consider other db
            // TODO consider no primary key
            MigrationRecordExtractor extractor = new FullRecordExtractor(table,
                sourceDataSource,
                positioner.getLast(),
                crawSize,
                runMode);

            return extractor;
        } else if (runMode == RunMode.MARK) {
            throw new CanalException("current not support MARK Run Mode.");
        } else if (runMode == RunMode.SYNC) {
            throw new CanalException("current not support SYNC Run Mode.");
        } else {
            throw new CanalException("not support other Run Mode ,current mode:" + runMode);
        }
    }

    protected MigrationRecordApplier chooseApplier(MigrationTable table, RunMode runMode, int dop, int batchSize) {
        // TODO consider other run mode
        // TODO consider other db
        MigrationRecordApplier applier = new BatchRecordApplier(dop,
            batchSize,
            applierExecutor,
            targetDataSource,
            table);
        return applier;
    }

    protected DataSource initDataSource(String type) {
        String username = config.getString(MigrationConstants.DATASOURCE_USERNAME_PREFIX + type);
        String password = config.getString(MigrationConstants.DATASOURCE_PASSWORD_PREFIX + type);
        DBType dbType = DBType.valueOf(config.getString(MigrationConstants.DATASOURCE_DBTYPE_PREFIX + type));
        String url = config.getString(MigrationConstants.DATASOURCE_URL_PREFIX + type);
        String encode = config.getString(MigrationConstants.DATASOURCE_ENCODE_PREFIX + type);
        String poolSize = config.getString(MigrationConstants.DATASOURCE_POOLSIZE_PREFIX + type);

        Properties prop = new Properties();
        if (poolSize != null) {
            prop.setProperty("maxActive", poolSize);
        } else {
            prop.setProperty("maxActive", "200");
        }

        if (dbType.isMySQL()) {
            prop.setProperty("characterEncoding", encode);
        }

        DataSourceConfig dsConfig = new DataSourceConfig(username, password, url, dbType, encode, prop);
        return dataSourceFactory.getDataSource(dsConfig);
    }

    protected List<MigrationTable> initTables() {
        // origin white table format is 'schema.table#...#...,schema.table2#...,...,...'
        List tableWhiteList = config.getList(MigrationConstants.TABLE_WHITE_LIST);
        List tableBlackList = config.getList(MigrationConstants.TABLE_BLACK_LIST);

        // check specified white tables are not all empty.
        boolean isEmpty = true;
        for (Object table : tableWhiteList) {
            isEmpty &= StringUtils.isBlank((String) table);
        }

        List<MigrationTable> rTables = Lists.newArrayList();
        if (!isEmpty) {
            for (Object obj : tableWhiteList) {
                String whiteTable = getTable((String) obj);
                if (!isBlackTable(whiteTable, tableBlackList)) {
                    String[] strs = StringUtils.split(whiteTable, ".");
                    List<MigrationTable> whiteTables = null;
                    if (strs.length == 1) {
                        // table expression is 'table'
                        whiteTables = TableMetaGenerator.getTableMetaWithoutColumn(sourceDataSource, null, strs[0]);
                    } else if (strs.length == 2) {
                        // table expression is 'schema.table'
                        whiteTables = TableMetaGenerator.getTableMetaWithoutColumn(sourceDataSource, strs[0], strs[1]);
                    } else {
                        throw new CanalException("table [" + whiteTable + "] is not valid");
                    }

                    if (whiteTables.isEmpty()) {
                        throw new CanalException("table [" + whiteTable + "] is not found");
                    }

                    // never in black table list will be migrate or sync
                    whiteTables.stream()
                        .filter(t -> !isBlackTable(t.getName(), tableBlackList)
                                     && !isBlackTable(t.getFullName(), tableBlackList))
                        .forEach(t -> {
                            TableMetaGenerator.buildColumns(sourceDataSource, (MigrationTable) t);
                            if (!rTables.contains((MigrationTable) t)) {
                                rTables.add((MigrationTable) t);
                            }
                        });
                }
            }
        } else {
            // table white list not set, fetch all table from database
            List<MigrationTable> metas = TableMetaGenerator.getTableMetaWithoutColumn(sourceDataSource, null, null);
            metas.stream()
                .filter(
                    t -> !isBlackTable(t.getName(), tableBlackList) && !isBlackTable(t.getFullName(), tableBlackList))
                .forEach(t -> {
                    TableMetaGenerator.buildColumns(sourceDataSource, (MigrationTable) t);
                    if (!rTables.contains((MigrationTable)t)) {
                        rTables.add((MigrationTable)t);
                    }
                });
        }

        return rTables;
    }

    /**
     * blackTable maybe a normal table or a table expression, whiteTable is a normal
     * table which was fetched from database metadata or a table specified in
     * config. check whiteTable whether match the blackTables and return the result
     * 
     * @param whiteTable
     * @param blackTables
     * @return
     */
    protected boolean isBlackTable(String whiteTable, List<String> blackTables) {
        for (String b : blackTables) {
            if (LikeUtil.isMatch(b, whiteTable)) {
                return true;
            }
        }
        return false;
    }

    /**
     * first element is tableName ,and should not be empty
     *
     * @param tableName
     * @return
     */
    protected String getTable(String tableName) {
        String[] paramArray = tableName.split("#");
        if (paramArray.length >= 1 && !"".equals(paramArray[0])) {
            return paramArray[0];
        } else {
            return null;
        }
    }

    public void waitForDone() throws InterruptedException {
        tableController.waitForDone();
    }

    @Override
    public void stop() {
        super.stop();
    }
}
