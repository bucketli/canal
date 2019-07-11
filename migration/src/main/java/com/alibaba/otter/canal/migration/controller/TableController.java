package com.alibaba.otter.canal.migration.controller;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * @author bucketli 2019/7/11 6:17 PM
 * @since 1.1.3
 **/
public class TableController {

    private CountDownLatch                         latch;
    private Semaphore                              sem;
    private LinkedBlockingQueue<MigrationInstance> queue = new LinkedBlockingQueue<MigrationInstance>();

    public TableController(int total, int concurrent){
        this.latch = new CountDownLatch(total);
        this.sem = new Semaphore(concurrent);
    }

    public void acquire() throws InterruptedException {
        sem.acquire();
    }

    public void release(MigrationInstance instance) {
        sem.release();
        queue.offer(instance);
        latch.countDown();
    }

    public MigrationInstance takeDone() throws InterruptedException {
        return queue.take();
    }

    public void waitForDone() throws InterruptedException {
        latch.await();
    }
}
