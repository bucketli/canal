package com.alibaba.otter.canal.migration.utils;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * a multi-thread executor template
 *
 * <pre>
 * e.g.,
 *
 * ExecutorTemplate template = new ExecutorTemplate(executor);
 * ...
 * try {
 *    for ( ....) {
 *       template.submit(new Runnable() {})
 *    }
 *
 *    List<?> result = template.waitForResult();
 *    // do result
 * } finally {
 *    template.clear();
 * }
 *
 * tips: concurrent use is not safe
 * </pre>
 *
 * @author agapple 2013-2-26 下午10:46:43
 */
public class ExecutorTemplate {

    private volatile ExecutorCompletionService completions = null;
    private volatile List<Future>              futures     = null;

    public ExecutorTemplate(ThreadPoolExecutor executor){
        futures = Collections.synchronizedList(Lists.newArrayList());
        completions = new ExecutorCompletionService(executor);
    }

    public void submit(Runnable task) {
        Future future = completions.submit(task, null);
        futures.add(future);
        check(future);
    }

    public void submit(Callable<Exception> task) {
        Future future = completions.submit(task);
        futures.add(future);
        check(future);
    }

    public synchronized List<?> waitForResult() {
        List result = Lists.newArrayList();
        RuntimeException exception = null;
        int index = 0;
        while (index < futures.size()) {
            try {
                //
                Future future = completions.take();
                result.add(future.get());
            } catch (Throwable e) {
                exception = new RuntimeException(e);
                break;
            }

            index++;
        }

        if (exception != null) {
            cancelAllFutures();
            throw exception;
        } else {
            return result;
        }
    }

    private void check(Future future) {
        if (future.isDone()) {
            // CallerRun strategy may done immediately,just do action once;
            try {
                future.get();
            } catch (Throwable e) {
                cancelAllFutures();
                throw new RuntimeException(e);
            }
        }
    }

    private void cancelAllFutures() {
        for (Future future : futures) {
            if (!future.isDone() && !future.isCancelled()) {
                future.cancel(true);
            }
        }
    }

    public void clear() {
        futures.clear();
    }
}
