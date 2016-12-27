package akka.persistence.ignite.journal;


import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import akka.persistence.AtomicWrite;
import akka.persistence.PersistentRepr;
import akka.persistence.ignite.extension.BaseStorage;
import akka.persistence.journal.japi.AsyncWriteJournal;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import scala.collection.JavaConverters;
import scala.concurrent.Future;

import javax.cache.Cache;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by anton on 17.10.16.
 */
@Slf4j
public class IgniteWriteJournal extends AsyncWriteJournal {

    private final Serializer serializer;
    private final BaseStorage<JournalItem> storage;

    public IgniteWriteJournal(Config config) {
        ActorSystem actorSystem = context().system();
        serializer = SerializationExtension.get(actorSystem).serializerFor(PersistentRepr.class);
        storage = new BaseStorage<>(config, actorSystem, JournalItem.class);
    }

    @Override
    public Future<Void> doAsyncReplayMessages(String persistenceId, long fromSequenceNr, long toSequenceNr, long max, Consumer<PersistentRepr> replayCallback) {
        return storage.execute(persistenceId, cache -> {
            log.debug("doAsyncReplayMessages '{}' {} {}", persistenceId, fromSequenceNr, toSequenceNr);
            try (QueryCursor<Cache.Entry<Long, JournalItem>> query = cache
                    .query(new SqlQuery<Long, JournalItem>(JournalItem.class, "sequenceNr >= ? AND sequenceNr <= ?")
                            .setArgs(fromSequenceNr, toSequenceNr))) {
                for (Cache.Entry<Long, JournalItem> entry : query) {
                    log.debug("replay message '{}' {}", persistenceId, entry.getKey());
                    replayCallback.accept(convert(entry.getValue()));
                }
            }
            return null;
        });
    }

    @Override
    public Future<Long> doAsyncReadHighestSequenceNr(String persistenceId, long fromSequenceNr) {
        return storage.execute(persistenceId, cache -> {
            List<List<?>> seq = cache
                    .query(new SqlFieldsQuery("select max(sequenceNr) from JournalItem where sequenceNr > ?").setArgs(fromSequenceNr))
                    .getAll();
            long highestSequenceNr = listsToStreamLong(seq).findFirst().orElse(fromSequenceNr);
            log.debug("doAsyncReadHighestSequenceNr '{}' {} -> {}", persistenceId, fromSequenceNr, highestSequenceNr);
            return highestSequenceNr;
        });
    }

    @Override
    public Future<Iterable<Optional<Exception>>> doAsyncWriteMessages(Iterable<AtomicWrite> messages) {
        return Futures.sequence(
                StreamSupport.stream(messages.spliterator(), false)
                        .map(this::writeBatch)
                        .collect(Collectors.toList()), storage.getDispatcher()
        );
    }

    private Future<Optional<Exception>> writeBatch(AtomicWrite atomicWrite) {
        return storage.execute(atomicWrite.persistenceId(), cache -> {
            try {
                Map<Long, JournalItem> batch = JavaConverters.seqAsJavaListConverter(atomicWrite.payload())
                        .asJava().stream()
                        .map(this::convert)
                        .collect(Collectors.toMap(JournalItem::getSequenceNr, item -> item));
                cache.putAll(batch);
                log.debug("doAsyncWriteMessages '{}': {}", atomicWrite.persistenceId(), batch);
                return Optional.empty();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                return Optional.of(e);
            }
        });
    }

    @Override
    public Future<Void> doAsyncDeleteMessagesTo(String persistenceId, long toSequenceNr) {
        return storage.execute(persistenceId, cache -> {
            log.debug("doAsyncDeleteMessagesTo '{}' {}", persistenceId, toSequenceNr);
            List<List<?>> seq = cache
                    .query(new SqlFieldsQuery("select sequenceNr from JournalItem where sequenceNr <= ?").setArgs(toSequenceNr))
                    .getAll();
            Set<Long> keys = listsToStreamLong(seq).collect(Collectors.toSet());
            log.debug("remove keys {}", keys);
            cache.removeAll(keys);
            return null;
        });
    }

    private JournalItem convert(PersistentRepr p) {
        return new JournalItem(p.sequenceNr(), serializer.toBinary(p));
    }

    private PersistentRepr convert(JournalItem item) {
        return (PersistentRepr) serializer.fromBinary(item.getPayload());
    }

    private static Stream<Long> listsToStreamLong(List<List<?>> list) {
        return list.stream().flatMap(Collection::stream).filter(o -> o instanceof Long).map(o -> (Long) o);
    }
}
