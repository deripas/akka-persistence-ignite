package akka.persistence.ignite.journal;


import akka.dispatch.ExecutionContexts;
import akka.dispatch.Futures;
import akka.persistence.AtomicWrite;
import akka.persistence.PersistentRepr;
import akka.persistence.ignite.extension.IgniteExtension;
import akka.persistence.journal.japi.AsyncWriteJournal;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import scala.collection.JavaConverters;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.Future;

import javax.cache.Cache;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by anton on 17.10.16.
 */
@Slf4j
public class IgniteWriteJournal extends AsyncWriteJournal {

    public static final String CACHE_PREFIX_PROPERTY = "cache-prefix";

    private final Serializer serializer;
    private final Ignite ignite;
    private final String cachePrefix;

    private final ExecutionContextExecutor contextExecutor;

    public IgniteWriteJournal(Config config) {
        serializer = SerializationExtension.get(context().system()).serializerFor(PersistentRepr.class);
        ignite = IgniteExtension.EXTENSION.get(context().system()).getIgnite();
        cachePrefix = config.getString(CACHE_PREFIX_PROPERTY);

        //todo fix multi-treading
        contextExecutor = ExecutionContexts.fromExecutor(Executors.newSingleThreadExecutor());
    }

    private IgniteCache<Long, JournalItem> getMapByPersistenceId(String persistenceId) {
        CacheConfiguration<Long, JournalItem> cfg = new CacheConfiguration<>(cachePrefix + persistenceId);
        // todo need configure
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setIndexedTypes(Long.class, JournalItem.class);
        return ignite.getOrCreateCache(cfg);
    }

    @Override
    public Future<Void> doAsyncReplayMessages(String persistenceId, long fromSequenceNr, long toSequenceNr, long max, Consumer<PersistentRepr> replayCallback) {
        return Futures.future(() -> {
            log.debug("doAsyncReplayMessages '{}' {} {}", persistenceId, fromSequenceNr, toSequenceNr);
            IgniteCache<Long, JournalItem> cache = getMapByPersistenceId(persistenceId);
            List<Cache.Entry<Long, JournalItem>> items = cache
                    .query(new SqlQuery<Long, JournalItem>(JournalItem.class, "sequenceNr >= ? AND sequenceNr <= ?").setArgs(fromSequenceNr, toSequenceNr))
                    .getAll();
            items.forEach(item -> replayCallback.accept(convert(item.getValue())));
            return null;
        }, contextExecutor);
    }

    @Override
    public Future<Long> doAsyncReadHighestSequenceNr(String persistenceId, long fromSequenceNr) {
        return Futures.future(() -> {
            IgniteCache<Long, JournalItem> cache = getMapByPersistenceId(persistenceId);
            List<List<?>> seq = cache
                    .query(new SqlFieldsQuery("select max(sequenceNr) from JournalItem where sequenceNr > ?").setArgs(fromSequenceNr))
                    .getAll();
            long highestSequenceNr = listsToStreamLong(seq).findFirst().orElse(fromSequenceNr);
            log.debug("doAsyncReadHighestSequenceNr '{}' {} -> {}", persistenceId, fromSequenceNr, highestSequenceNr);
            return highestSequenceNr;
        }, contextExecutor);
    }

    @Override
    public Future<Iterable<Optional<Exception>>> doAsyncWriteMessages(Iterable<AtomicWrite> messages) {
        return Futures.future(() -> StreamSupport.stream(messages.spliterator(), false)
                .map(this::writeBatch)
                .collect(Collectors.toList()), contextExecutor);
    }

    private Optional<Exception> writeBatch(AtomicWrite atomicWrite) {
        try {
            Map<Long, JournalItem> batch = JavaConverters.seqAsJavaListConverter(atomicWrite.payload()).asJava().stream()
                    .collect(Collectors.toMap(PersistentRepr::sequenceNr, this::convert));
            getMapByPersistenceId(atomicWrite.persistenceId()).putAll(batch);
            log.debug("doAsyncWriteMessages '{}': {}", atomicWrite.persistenceId(), batch);
            return Optional.empty();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return Optional.of(e);
        }
    }

    @Override
    public Future<Void> doAsyncDeleteMessagesTo(String persistenceId, long toSequenceNr) {
        return Futures.future(() -> {
            log.debug("doAsyncDeleteMessagesTo '{}' {}", persistenceId, toSequenceNr);
            IgniteCache<Long, JournalItem> cache = getMapByPersistenceId(persistenceId);
            List<List<?>> seq = cache
                    .query(new SqlFieldsQuery("select sequenceNr from JournalItem where sequenceNr <= ?").setArgs(toSequenceNr))
                    .getAll();
            Set<Long> keys = listsToStreamLong(seq).collect(Collectors.toSet());
            log.debug("remove keys {}", keys);
            cache.removeAll(keys);
            return null;
        }, contextExecutor);
    }

    private Stream<Long> listsToStreamLong(List<List<?>> list) {
        return list.stream().flatMap(Collection::stream).filter(o -> o instanceof Long).map(o -> (Long) o);
    }

    private JournalItem convert(PersistentRepr p) {
        return new JournalItem(p.sequenceNr(), serializer.toBinary(p));
    }

    private PersistentRepr convert(JournalItem item) {
        return (PersistentRepr) serializer.fromBinary(item.getPayload());
    }
}
