package akka.persistence.ignite.snapshot;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Set;

/**
 * Created by anton on 01.11.16.
 */
public class SnapshotItemCacheTest {

    private Ignite ignite;
    private SnapshotItemCache cache;

    @BeforeClass
    public void init() {
        ignite = Ignition.start();
        cache = new SnapshotItemCache(ignite, "test");
    }

    @AfterClass
    public void destroy() {
        ignite.close();
    }

    @Test
    public void test1() {
        cache.save("a", new SnapshotItem(1L, 0L, new byte[64]));
        cache.save("a", new SnapshotItem(2L, 0L, new byte[64]));

        Assert.assertTrue(cache.remove("a", 1L));
        Assert.assertTrue(cache.remove("a", 2L));
        Assert.assertFalse(cache.remove("a", 2L));
        Assert.assertFalse(cache.remove("a", 3L));
    }

    @Test
    public void test2() {
        cache.save("a", new SnapshotItem(1L, 0L, new byte[64]));
        cache.save("a", new SnapshotItem(2L, 0L, new byte[64]));

        Set<Long> remove = cache.remove("a", 0, Long.MAX_VALUE, 0, Long.MAX_VALUE);
        Assert.assertEquals(remove.size(), 2);
        Assert.assertTrue(remove.contains(1L));
        Assert.assertTrue(remove.contains(2L));
    }

    @Test
    public void test3() {
        cache.save("a", new SnapshotItem(1L, 0L, new byte[64]));
        cache.save("a", new SnapshotItem(2L, 0L, new byte[64]));

        SnapshotItem item = cache.findLast("a", 0, Long.MAX_VALUE, 0, Long.MAX_VALUE);
        Assert.assertNotNull(item);
        Assert.assertEquals((long) item.getSequenceNr(), 2L);

        Assert.assertNull(cache.findLast("b", 0, Long.MAX_VALUE, 0, Long.MAX_VALUE));
    }
}
