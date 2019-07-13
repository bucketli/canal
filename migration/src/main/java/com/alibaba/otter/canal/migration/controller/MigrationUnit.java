package com.alibaba.otter.canal.migration.controller;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.migration.applier.MigrationRecordApplier;
import com.alibaba.otter.canal.migration.extractor.MigrationRecordExtractor;
import com.alibaba.otter.canal.migration.metadata.MigrationTable;
import com.alibaba.otter.canal.migration.model.KeyPosition;
import com.alibaba.otter.canal.migration.process.RecordPositioner;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.sql.DataSource;
import java.util.concurrent.CountDownLatch;

/**
 * @author bucketli 2019-06-30 10:56
 * @since 1.1.3
 */
public class MigrationUnit extends AbstractCanalLifeCycle {

    public static final String             MDC_TABLE_KEY = "table";
    private static final Logger            logger                = LoggerFactory.getLogger(MigrationUnit.class);
    private final MigrationRecordExtractor extractor;
    private final MigrationRecordApplier   applier;
    private final RecordPositioner         positioner;
    private final TableController          tableController;
    private final MigrationTable           table;
    private final DataSource               originDS;
    private final DataSource               targetDS;

    private CanalException                 exception;
    private CountDownLatch                 mutex;
    private Thread                         worker;

    public MigrationUnit(MigrationRecordExtractor extractor, MigrationRecordApplier applier,
                         RecordPositioner positioner, TableController tableController, MigrationTable table,
                         DataSource originDS, DataSource targetDS){
        this.extractor = extractor;
        this.applier = applier;
        this.positioner = positioner;
        this.tableController = tableController;
        this.table = table;
        this.originDS = originDS;
        this.targetDS = targetDS;
    }

    @Override
    public void start() {
        MDC.put(MDC_TABLE_KEY, table.getFullName());
        super.start();

        try {
            tableController.acquire();

            if (!positioner.isStart()) {
                positioner.start();
            }

            KeyPosition position = positioner.getLast();

            if (!extractor.isStart()) {
                extractor.start();
            }

            if (!applier.isStart()) {
                applier.start();
            }

            worker = new Thread(new Runnable() {

                @Override
                public void run() {
                    logger.info("table {} is start",table.getFullName());
                    MDC.remove(MDC_TABLE_KEY);


                }
            });

            worker.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {

                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    logger.error("uncaught exception,root cause message: " + ExceptionUtils.getRootCauseMessage(e), e);
                }
            });
            worker.setName(this.getClass().getSimpleName() + "-" + table.getFullName());
            worker.start();

            logger.info("table {} start successful,extractor:{},applier:{}",
                new Object[] { table.getFullName(), extractor.getClass().getName(), applier.getClass().getName() });

        } catch (InterruptedException e) {
            exception = new CanalException(e);
            mutex.countDown();
            tableController.release(this);
            Thread.currentThread().interrupt();
        } catch (Throwable e) {
            exception = new CanalException(e);
            mutex.countDown();
            logger.error("table {} failed ,caused by {}", table.getFullName(), ExceptionUtils.getFullStackTrace(e));
            tableController.release(this);
        }

    }

    public void waitForDone() throws InterruptedException, CanalException {
        mutex.await();

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public void stop() {
        super.stop();
    }
}
