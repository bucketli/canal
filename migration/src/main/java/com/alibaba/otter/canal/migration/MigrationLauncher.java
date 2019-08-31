package com.alibaba.otter.canal.migration;

import com.alibaba.otter.canal.migration.controller.MigrationController;
import com.alibaba.otter.canal.migration.process.MigrationConstants;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * @author bucketli 2019-06-30 10:42
 * @since 1.1.3
 */
public class MigrationLauncher {

    private static final Logger logger = LoggerFactory.getLogger(MigrationLauncher.class);

    public static void main(String[] args) {
        try {
            String conf = System.getProperty("migration.conf", "classpath:migration.properties");
            PropertiesConfiguration config = new PropertiesConfiguration();
            if (conf.startsWith(MigrationConstants.CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, MigrationConstants.CLASSPATH_URL_PREFIX);
                Reader r = new InputStreamReader(MigrationLauncher.class.getClassLoader().getResourceAsStream(conf),
                    "UTF-8");
                config.read(r);
            } else {
                Reader r = new InputStreamReader(new FileInputStream(conf));
                config.read(r);
            }

            final MigrationController controller = new MigrationController(config);
            controller.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    if (controller.isStart()) {
                        controller.stop();
                    }
                } catch (Throwable e) {
                    logger.error("stop controller error.", ExceptionUtils.getFullStackTrace(e));
                } finally {
                    logger.info("migration is down.");
                }
            }));

            controller.waitForDone();
            Thread.sleep(3000);
            if (controller.isStart()) {
                controller.stop();
            }
        } catch (Throwable e) {
            logger.error("start up error.", ExceptionUtils.getFullStackTrace(e));
            System.exit(0);
        }

    }
}
