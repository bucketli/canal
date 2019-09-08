package com.alibaba.otter.canal.migration.controller;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @author bucketli 2019/7/11 6:17 PM
 * @since 1.1.3
 **/
public class TableController {

    private CountDownLatch latch;
    private Semaphore sem;
    private LinkedBlockingQueue<MigrationUnit> queue = new LinkedBlockingQueue<MigrationUnit>();

    public TableController(int total, int concurrent) {
        this.latch = new CountDownLatch(total);
        this.sem = new Semaphore(concurrent);
    }

    public void acquire() throws InterruptedException {
        sem.acquire();
    }

    protected boolean acquire(int milliseconds) throws InterruptedException {
        return sem.tryAcquire(milliseconds, TimeUnit.MILLISECONDS);
    }

    public void release(MigrationUnit unit) {
        sem.release();
        queue.offer(unit);
        latch.countDown();
    }

    public MigrationUnit takeDone() throws InterruptedException {
        return queue.take();
    }

    public void waitForDone() throws InterruptedException {
        latch.await();
    }

    protected boolean waitForDone(int milliseconds) throws InterruptedException {
        return latch.await(milliseconds, TimeUnit.MILLISECONDS);
    }
}
