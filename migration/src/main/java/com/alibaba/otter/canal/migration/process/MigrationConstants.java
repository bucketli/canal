package com.alibaba.otter.canal.migration.process;

/**
 * @author bucketli 2019/7/30 7:05 PM
 * @since 1.1.3
 **/
public class MigrationConstants {

    public static final String CLASSPATH_URL_PREFIX          = "classpath:";

    public static final String TARGET                        = "target";

    public static final String SOURCE                        = "source";

    public static final String RUN_MODE                      = "canal.migration.mode";

    public static final String SOURCE_DATABASE_TYPE          = "canal.migration.source_dbtype";

    public static final String TARGET_DATABASE_TYPE          = "canal.migration.target_dbtype";

    public static final String ETL_CONCURRENT_SIZE           = "canal.migration.etl.concurrent_size";

    public static final String ETL_APPLIER_CONCURRENT_SIZE   = "canal.migration.etl.applier.concurrent_size";

    public static final String EXTRACTOR_CRAW_SIZE           = "canal.migration.etl.extractor.craw_size";

    public static final String APPLIER_BATCH_SIZE            = "canal.migration.etl.applier.batch_size";

    public static final String TABLE_WHITE_LIST              = "canal.instance.filter.regex";

    public static final String TABLE_BLACK_LIST              = "canal.instance.filter.black.regex";

    public static final String DATASOURCE_USERNAME_SOURCE    = "canal.instance.dbUsername";

    public static final String DATASOURCE_PASSWORD_SOURCE    = "canal.instance.dbPassword";

    public static final String DATASOURCE_URL_SOURCE         = "canal.instance.master.address";

    public static final String DATASOURCE_DBTYPE_SOURCE      = "canal.migration.database.source.dbtype";

    public static final String DATASOURCE_ENCODE_SOURCE      = "canal.migration.database.source.encode";

    public static final String DATASOURCE_POOLSIZE_SOURCE    = "canal.migration.database.source.poolsize";

    public static final String DATASOURCE_USERNAME_TARGET    = "canal.migration.database.target.username";

    public static final String DATASOURCE_PASSWORD_TARGET    = "canal.migration.database.target.password";

    public static final String DATASOURCE_DBTYPE_TARGET      = "canal.migration.database.target.dbtype";

    public static final String DATASOURCE_URL_TARGET         = "canal.migration.database.target.url";

    public static final String DATASOURCE_ENCODE_TARGET      = "canal.migration.database.target.encode";

    public static final String DATASOURCE_POOLSIZE_TARGET    = "canal.migration.database.target.poolsize";

    public static final String POSITION_FILE_DIR             = "../conf/positioner";
}
