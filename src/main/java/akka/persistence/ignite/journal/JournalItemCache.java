package akka.persistence.ignite.journal;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import javax.cache.Cache;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by anton on 01.11.16.
 */
public class JournalItemCache {

    private final Ignite ignite;
    private final String cachePrefix;

    public JournalItemCache(Ignite ignite, String cachePrefix) {
        this.ignite = ignite;
        this.cachePrefix = cachePrefix;
    }

    private IgniteCache<Long, JournalItem> getCacheByPersistenceId(String persistenceId) {
        CacheConfiguration<Long, JournalItem> cfg = new CacheConfiguration<>(cachePrefix + persistenceId);
        // todo need configure
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setIndexedTypes(Long.class, JournalItem.class);
        return ignite.getOrCreateCache(cfg);
    }

    public void put(String persistenceId, Collection<JournalItem> items) {
        IgniteCache<Long, JournalItem> cache = getCacheByPersistenceId(persistenceId);
        cache.putAll(items.stream().collect(Collectors.toMap(JournalItem::getSequenceNr, item -> item)));
    }

    public List<JournalItem> get(String persistenceId, long fromSequenceNr, long toSequenceNr) {
        IgniteCache<Long, JournalItem> cache = getCacheByPersistenceId(persistenceId);
        List<JournalItem> items = new ArrayList<>();
        try (QueryCursor<Cache.Entry<Long, JournalItem>> query = cache
                .query(new SqlQuery<Long, JournalItem>(JournalItem.class, "sequenceNr >= ? AND sequenceNr <= ?")
                        .setArgs(fromSequenceNr, toSequenceNr))) {
            for (Cache.Entry<Long, JournalItem> entry : query) {
                items.add(entry.getValue());
            }
        }
        return items;
    }

    public Set<Long> removeTo(String persistenceId, long toSequenceNr) {
        IgniteCache<Long, JournalItem> cache = getCacheByPersistenceId(persistenceId);
        List<List<?>> seq = cache
                .query(new SqlFieldsQuery("select sequenceNr from JournalItem where sequenceNr <= ?").setArgs(toSequenceNr))
                .getAll();
        Set<Long> keys = listsToStreamLong(seq).collect(Collectors.toSet());
        cache.removeAll(keys);
        return keys;
    }

    public long getMaxSequenceNr(String persistenceId, long fromSequenceNr) {
        IgniteCache<Long, JournalItem> cache = getCacheByPersistenceId(persistenceId);
        List<List<?>> seq = cache
                .query(new SqlFieldsQuery("select max(sequenceNr) from JournalItem where sequenceNr > ?").setArgs(fromSequenceNr))
                .getAll();
        return listsToStreamLong(seq).findFirst().orElse(fromSequenceNr);
    }

    private Stream<Long> listsToStreamLong(List<List<?>> list) {
        return list.stream().flatMap(Collection::stream).filter(o -> o instanceof Long).map(o -> (Long) o);
    }
}
