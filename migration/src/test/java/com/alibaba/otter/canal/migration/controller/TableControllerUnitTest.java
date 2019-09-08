package com.alibaba.otter.canal.migration.controller;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.migration.controller.MigrationUnit;
import com.alibaba.otter.canal.migration.controller.TableController;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * @author bucketli 2019-07-13 09:50
 * @since 1.1.3
 */
public class TableControllerUnitTest {
    boolean re = false;

    private List<MigrationUnit> getMigrationUnit(int count) {
        List<MigrationUnit> r = Lists.newArrayListWithCapacity(count);
        for (int i = 0; i < count; i++) {
            MigrationUnit u = new MigrationUnit(null, null, null, null, null);
            r.add(u);
        }

        return r;
    }

    @Test
    public void testAcquire() {
        TableController t = new TableController(1, 2);
        re = false;

        try {
            t.acquire();
        } catch (InterruptedException e) {
            Assert.fail();
        }

        Thread worker = new Thread(() -> {
            try {
                t.acquire();
            } catch (InterruptedException e1) {
                Assert.fail();
            }
        });

        Thread worker2 = new Thread(() -> {
            try {
                re = t.acquire(1000);
            } catch (InterruptedException e1) {
                Assert.fail();
            }
        });

        worker.start();
        worker2.start();

        try {
            worker.join();
            worker2.join();
        } catch (InterruptedException e) {
            Assert.fail();
        }

        Assert.assertFalse(re);
    }

    @Test
    public void testRelease() {
        TableController t = new TableController(3, 2);
        re = false;
        List<MigrationUnit> unit = getMigrationUnit(1);

        try {
            t.acquire();
        } catch (InterruptedException e) {
            Assert.fail();
        }

        Thread worker = new Thread(() -> {
            try {
                t.acquire();
            } catch (InterruptedException e1) {
                Assert.fail();
            }
        });

        Thread worker2 = new Thread(() -> {
            try {
                re = t.acquire(1000);
            } catch (InterruptedException e1) {
                Assert.fail();
            }
        });

        worker.start();
        t.release(unit.get(0));
        worker2.start();

        try {
            worker.join();
            worker2.join();
        } catch (InterruptedException e) {
            Assert.fail();
        }

        Assert.assertTrue(re);
    }

    @Test
    public void testTakeDone() {
        TableController t = new TableController(3, 2);
        List<MigrationUnit> unit = getMigrationUnit(3);

        try {
            t.acquire();
            t.acquire();
            t.release(unit.get(0));
            t.release(unit.get(1));
            Assert.assertEquals(unit.get(0),t.takeDone());
            Assert.assertEquals(unit.get(1),t.takeDone());
            t.acquire();
            t.release(unit.get(2));
            Assert.assertEquals(unit.get(2),t.takeDone());
        }catch(InterruptedException e){
            Assert.fail();
        }
    }

    @Test
    public void testWaitDone() {
        TableController t = new TableController(3, 2);
        List<MigrationUnit> unit = getMigrationUnit(3);

        try {
            t.acquire();
            t.acquire();
            Assert.assertFalse(t.waitForDone(200));
            t.release(unit.get(0));
            Assert.assertFalse(t.waitForDone(200));
            t.release(unit.get(1));
            Assert.assertFalse(t.waitForDone(200));
            t.acquire();
            t.release(unit.get(2));
            t.waitForDone();
        }catch(InterruptedException e){
            Assert.fail();
        }
    }
}
