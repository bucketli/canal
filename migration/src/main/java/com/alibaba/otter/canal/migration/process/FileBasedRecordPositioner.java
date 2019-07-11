package com.alibaba.otter.canal.migration.process;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.migration.model.KeyPosition;
import org.springframework.util.Assert;

/**
 * @author bucketli 2019/7/8 1:46 PM
 * @since 1.1.3
 **/
public class FileBasedRecordPositioner extends AbstractCanalLifeCycle implements RecordPositioner {

    private static final Logger      logger           = LoggerFactory.getLogger(FileBasedRecordPositioner.class);
    private static final Charset     charset          = Charset.forName("UTF-8");
    private File                     positionDir;
    private String                   positionFileName = "position.dat";
    private File                     positionFile;
    private ScheduledExecutorService executor;
    private long                     period           = 100;

    private AtomicBoolean            needFlush        = new AtomicBoolean(false);
    private AtomicBoolean            needReload       = new AtomicBoolean(true);

    protected volatile KeyPosition   position;

    public void start() {
        Assert.notNull(positionDir);
        if (!positionDir.exists()) {
            try {
                FileUtils.forceMkdir(positionDir);
            } catch (IOException e) {
                throw new CanalException(e);
            }
        }

        if (!positionDir.canRead() || !positionDir.canWrite()) {
            throw new CanalException(positionDir.getPath() + " can not read/write ");
        }

        positionFile = new File(positionDir, positionFileName);
        executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                if (needFlush.compareAndSet(true, false)) {
                    flushToFile(positionFile, getLast());
                }
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    public KeyPosition getLast() {
        if (needReload.compareAndSet(true, false)) {
            KeyPosition position = loadFromFile(positionFile);
            this.position = position;
        }

        return this.position;
    }

    public void persist(KeyPosition position) throws CanalException {
        needFlush.set(true);
        this.position = position;
    }

    public void stop() {
        flushToFile(positionFile, this.position);
        executor.shutdownNow();
    }

    private void flushToFile(File positionFile, KeyPosition position) {
        if (position != null) {
            try {
                String json = JSON
                    .toJSONString(position, SerializerFeature.WriteClassName, SerializerFeature.WriteNullListAsEmpty);
                FileUtils.writeStringToFile(positionFile, json);
            } catch (IOException e) {
                throw new CanalException(e);
            }
        }
    }

    private KeyPosition loadFromFile(File positionFile) {
        try {
            if (!positionFile.exists()) {
                return null;
            }

            String json = FileUtils.readFileToString(positionFile, charset.name());
            return JSON.parseObject(json, KeyPosition.class);
        } catch (IOException e) {
            throw new CanalException(e);
        }
    }
}
