package com.alibaba.otter.canal.migration.process;

/**
 * @author bucketli 2019/7/30 7:05 PM
 * @since 1.1.3
 **/
public class MigrationConstants {

    public static final String CLASSPATH_URL_PREFIX          = "classpath:";

    public static final String RUN_MODE                      = "canal.migration.mode";

    public static final String SOURCE_DATABASE_TYPE          = "canal.migration.source_dbtype";

    public static final String TARGET_DATABASE_TYPE          = "canal.migration.target_dbtype";

    public static final String ETL_CONCURRENT_SIZE           = "canal.migration.etl.concurrent_size";

    public static final String ETL_EXTRACTOR_CONCURRENT_SIZE = "canal.migration.etl.extractor.concurrent_size";

    public static final String ETL_APPLIER_CONCURRENT_SIZE   = "canal.migration.etl.applier.concurrent_size";

    public static final String EXTRACTOR_CRAW_SIZE           = "canal.migration.etl.extractor.craw_size";

    public static final String APPLIER_BATCH_SIZE            = "canal.migration.etl.applier.batch_size";

    public static final String TABLE_WHITE_LIST              = "canal.migration.table.white_list";

    public static final String TABLE_BLACK_LIST              = "canal.migration.table.black_list";

    public static final String TARGET                        = "target";

    public static final String SOURCE                        = "source";

    public static final String DATASOURCE_USERNAME_PREFIX    = "canal.migration.database.username.";

    public static final String DATASOURCE_PASSWORD_PREFIX    = "canal.migration.database.password.";

    public static final String DATASOURCE_DBTYPE_PREFIX      = "canal.migration.database.dbtype.";

    public static final String DATASOURCE_URL_PREFIX         = "canal.migration.database.url.";

    public static final String DATASOURCE_ENCODE_PREFIX      = "canal.migration.database.encode.";

    public static final String DATASOURCE_POOLSIZE_PREFIX    = "canal.migration.database.poolsize.";

    public static final String POSITION_FILE_DIR             = "../conf/positioner";
}
