package com.alibaba.otter.canal.migration.utils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author bucketli 2019-07-06 23:08
 * @since 1.1.3
 */
public class ExecutorTemplateTest {

    public void test() {
        ThreadPoolExecutor tpe = new ThreadPoolExecutor(1,
            2,
            60,
            TimeUnit.MINUTES,
            new ArrayBlockingQueue<Runnable>(2),
            new ThreadPoolExecutor.CallerRunsPolicy());
        ExecutorTemplate template = new ExecutorTemplate(tpe);
    }
}
