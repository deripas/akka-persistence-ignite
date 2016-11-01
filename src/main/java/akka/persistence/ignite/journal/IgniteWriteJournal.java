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
import scala.collection.JavaConverters;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.Future;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Created by anton on 17.10.16.
 */
@Slf4j
public class IgniteWriteJournal extends AsyncWriteJournal {

    public static final String CACHE_PREFIX_PROPERTY = "cache-prefix";

    private final JournalItemCache journalItemCache;
    private final ExecutionContextExecutor contextExecutor;
    private final Serializer serializer;

    public IgniteWriteJournal(Config config) {
        Ignite ignite = IgniteExtension.EXTENSION.get(context().system()).getIgnite();
        String cachePrefix = config.getString(CACHE_PREFIX_PROPERTY);
        journalItemCache = new JournalItemCache(ignite, cachePrefix);

        serializer = SerializationExtension.get(context().system()).serializerFor(PersistentRepr.class);

        //todo fix multi-treading
        contextExecutor = ExecutionContexts.fromExecutor(Executors.newSingleThreadExecutor());
    }

    @Override
    public Future<Void> doAsyncReplayMessages(String persistenceId, long fromSequenceNr, long toSequenceNr, long max, Consumer<PersistentRepr> replayCallback) {
        return Futures.future(() -> {
            log.debug("doAsyncReplayMessages '{}' {} {}", persistenceId, fromSequenceNr, toSequenceNr);
            List<JournalItem> items = journalItemCache.get(persistenceId, fromSequenceNr, toSequenceNr);
            items.forEach(item -> replayCallback.accept(convert(item)));
            return null;
        }, contextExecutor);
    }

    @Override
    public Future<Long> doAsyncReadHighestSequenceNr(String persistenceId, long fromSequenceNr) {
        return Futures.future(() -> {
            long highestSequenceNr = journalItemCache.getMaxSequenceNr(persistenceId, fromSequenceNr);
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
            List<JournalItem> batch = JavaConverters.seqAsJavaListConverter(atomicWrite.payload()).asJava().stream()
                    .map(this::convert)
                    .collect(Collectors.toList());
            journalItemCache.put(atomicWrite.persistenceId(), batch);
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
            Set<Long> keys = journalItemCache.removeTo(persistenceId, toSequenceNr);
            log.debug("remove keys {}", keys);
            return null;
        }, contextExecutor);
    }

    private JournalItem convert(PersistentRepr p) {
        return new JournalItem(p.sequenceNr(), serializer.toBinary(p));
    }

    private PersistentRepr convert(JournalItem item) {
        return (PersistentRepr) serializer.fromBinary(item.getPayload());
    }
}
