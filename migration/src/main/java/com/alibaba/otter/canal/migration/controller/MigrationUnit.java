package com.alibaba.otter.canal.migration.controller;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.migration.applier.MigrationRecordApplier;
import com.alibaba.otter.canal.migration.extractor.MigrationRecordExtractor;
import com.alibaba.otter.canal.migration.metadata.MigrationTable;
import com.alibaba.otter.canal.migration.model.KeyPosition;
import com.alibaba.otter.canal.migration.model.MigrationRecord;
import com.alibaba.otter.canal.migration.process.ExtractStatus;
import com.alibaba.otter.canal.migration.process.RecordPositioner;
import com.google.common.base.Predicates;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author bucketli 2019-06-30 10:56
 * @since 1.1.3
 */
public class MigrationUnit extends AbstractCanalLifeCycle {

    public static final String             MDC_TABLE_KEY = "table";
    private static final Logger            logger        = LoggerFactory.getLogger(MigrationUnit.class);
    private final MigrationRecordExtractor extractor;
    private final MigrationRecordApplier   applier;
    private final RecordPositioner         positioner;
    private final TableController          tableController;
    private final MigrationTable           table;

    private CanalException                 exception;
    private CountDownLatch                 mutex;
    private Thread                         worker;

    public MigrationUnit(MigrationRecordExtractor extractor, MigrationRecordApplier applier,
                         RecordPositioner positioner, TableController tableController, MigrationTable table){
        this.extractor = extractor;
        this.applier = applier;
        this.positioner = positioner;
        this.tableController = tableController;
        this.table = table;
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
                    try {
                        logger.info("table {} is start", table.getFullName());

                        try {
                            processTable();
                            exception = null;
                        } catch (CanalException e) {
                            exception = e;
                        }

                        if (exception == null) {
                            logger.info("table {} is end", table.getName());
                        } else if (ExceptionUtils.getRootCause(exception) instanceof InterruptedException) {
                            logger.info("table {} is interrupted, current status:{}",
                                table.getName(),
                                extractor.status());
                        } else {
                            logger.info("table {} is error,current status:{}", table.getName(), extractor.status());
                        }
                    } finally {
                        tableController.release(MigrationUnit.this);
                        mutex.countDown();
                    }
                }

                private void processTable() {
                    KeyPosition lastPosition = positioner.getLast();
                    ExtractStatus status = ExtractStatus.NORMAL;
                    do {
                        // extract one batch records
                        List<MigrationRecord> rs = extractor.extract();
                        // ack backup
                        List<MigrationRecord> ackRs = rs;
                        // check if empty
                        if (rs == null || rs.size() == 0) {
                            status = extractor.status();
                        }

                        applier.apply(rs);

                        KeyPosition ackPosition = extractor.ack(ackRs);
                        if (ackPosition != null) {
                            positioner.persist(ackPosition);
                        }
                    } while (status != ExtractStatus.TABLE_END);
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

        if (worker != null) {
            worker.interrupt();
            try {
                worker.join(2 * 1000);
            } catch (InterruptedException e) {
                // ignore
            }
        }

        if (extractor.isStart()) {
            extractor.stop();
        }

        if (applier.isStart()) {
            applier.stop();
        }

        if (positioner.isStart()) {
            positioner.stop();
        }

    }
}
