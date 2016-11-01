package akka.persistence.ignite.snapshot;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import javax.annotation.Nullable;
import javax.cache.Cache;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by anton on 01.11.16.
 */
public class SnapshotItemCache {
    private final Ignite ignite;
    private final String cachePrefix;

    public SnapshotItemCache(Ignite ignite, String cachePrefix) {
        this.ignite = ignite;
        this.cachePrefix = cachePrefix;
    }

    public IgniteCache<Long, SnapshotItem> getCacheByPersistenceId(String persistenceId) {
        CacheConfiguration<Long, SnapshotItem> cfg = new CacheConfiguration<>(cachePrefix + persistenceId);
        // todo need configure
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setIndexedTypes(Long.class, SnapshotItem.class);
        return ignite.getOrCreateCache(cfg);
    }

    public void save(String persistenceId, SnapshotItem item) {
        IgniteCache<Long, SnapshotItem> cache = getCacheByPersistenceId(persistenceId);
        cache.put(item.getSequenceNr(), item);
    }

    @Nullable
    public SnapshotItem findLast(String persistenceId, long minSequenceNr, long maxSequenceNr, long minTimestamp, long maxTimestamp) {
        IgniteCache<Long, SnapshotItem> cache = getCacheByPersistenceId(persistenceId);
        try (QueryCursor<Cache.Entry<Long, SnapshotItem>> query = cache
                .query(new SqlQuery<Long, SnapshotItem>(SnapshotItem.class, "sequenceNr >= ? AND sequenceNr <= ? AND timestamp >= ? AND timestamp <= ?")
                        .setArgs(minSequenceNr, maxSequenceNr, minTimestamp, maxTimestamp))) {
            Iterator<Cache.Entry<Long, SnapshotItem>> iterator = query.iterator();
            return iterator.hasNext() ? iterator.next().getValue() : null;
        }
    }

    public boolean remove(String persistenceId, long sequenceNr) {
        IgniteCache<Long, SnapshotItem> cache = getCacheByPersistenceId(persistenceId);
        return cache.remove(sequenceNr);
    }

    public Set<Long> remove(String persistenceId, long minSequenceNr, long maxSequenceNr, long minTimestamp, long maxTimestamp) {
        IgniteCache<Long, SnapshotItem> cache = getCacheByPersistenceId(persistenceId);
        List<List<?>> seq = cache
                .query(new SqlFieldsQuery("select sequenceNr from SnapshotItem where sequenceNr >= ? AND sequenceNr <= ? AND timestamp >= ? AND timestamp <= ?")
                        .setArgs(minSequenceNr, maxSequenceNr, minTimestamp, maxTimestamp))
                .getAll();
        Set<Long> keys = listsToStreamLong(seq).collect(Collectors.toSet());
        cache.removeAll(keys);
        return keys;
    }

    private Stream<Long> listsToStreamLong(List<List<?>> list) {
        return list.stream().flatMap(Collection::stream).filter(o -> o instanceof Long).map(o -> (Long) o);
    }

}
