package akka.persistence.ignite.journal;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Created by anton on 01.11.16.
 */
public class JournalItemCacheTest {

    private Ignite ignite;
    private JournalItemCache cache;

    @BeforeClass
    public void init() {
        ignite = Ignition.start();
        cache = new JournalItemCache(ignite, "test");
    }

    @AfterClass
    public void destroy() {
        ignite.close();
    }

    @Test
    public void test1() {
        cache.put("a", Arrays.asList(new JournalItem(1L, new byte[64]), new JournalItem(2L, new byte[64]), new JournalItem(3L, new byte[64])));

        long maxSequenceNr = cache.getMaxSequenceNr("a", 1L);
        Assert.assertEquals(maxSequenceNr, 3L);

        Set<Long> remove = cache.removeTo("a", 3L);
        Assert.assertEquals(remove.size(), 3);
    }

    @Test
    public void test2() {
        cache.put("a", Arrays.asList(new JournalItem(1L, new byte[64]), new JournalItem(2L, new byte[64]), new JournalItem(3L, new byte[64])));

        List<JournalItem> list = cache.get("a", 1, 2L);
        Assert.assertEquals(list.size(), 2);

        Set<Long> remove = cache.removeTo("a", 3L);
        Assert.assertEquals(remove.size(), 3);
    }
}
