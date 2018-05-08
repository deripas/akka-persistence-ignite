package akka.persistence.ignite.journal;


import akka.actor.ActorSystem;
import akka.persistence.AtomicWrite;
import akka.persistence.PersistentRepr;
import akka.persistence.ignite.extension.IgniteStorage;
import akka.persistence.journal.japi.AsyncWriteJournal;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import com.google.common.collect.Streams;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import scala.concurrent.Future;

import java.io.NotSerializableException;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static scala.compat.java8.FutureConverters.toScala;
import static scala.compat.java8.ScalaStreamSupport.stream;

/**
 * Created by anton on 17.10.16.
 */
@Slf4j
public class IgniteWriteJournal extends AsyncWriteJournal {

    public static final String IGNITE_JOURNAL_STORAGE = "ignite-journal-storage";

    private final Serializer serializer;
    private final IgniteStorage<JournalItem> storage;

    public IgniteWriteJournal(Config config) throws NotSerializableException {
        ActorSystem system = context().system();
        serializer = SerializationExtension.get(system).serializerFor(PersistentRepr.class);
        storage = new IgniteStorage<>(config, system, IGNITE_JOURNAL_STORAGE);
    }

    @Override
    public Future<Void> doAsyncReplayMessages(String persistenceId, long fromSequenceNr, long toSequenceNr, long max, Consumer<PersistentRepr> replayCallback) {
        log.debug("doAsyncReplayMessages '{}' {} {}", persistenceId, fromSequenceNr, toSequenceNr);
        String sql = "SELECT _val FROM JournalItem WHERE persistenceId = ? AND sequenceNr >= ? AND sequenceNr <= ?";
        return toScala(storage.sql(sql, JournalItem.class, persistenceId, fromSequenceNr, toSequenceNr)
                .thenAccept(items -> items.forEach(item -> {
                    log.debug("replay message '{}' {}", persistenceId, item);
                    replayCallback.accept(convert(item));
                })));
    }

    @Override
    public Future<Long> doAsyncReadHighestSequenceNr(String persistenceId, long fromSequenceNr) {
        log.debug("doAsyncReadHighestSequenceNr '{}' {}", persistenceId, fromSequenceNr);
        String sql = "SELECT MAX(sequenceNr) FROM JournalItem WHERE persistenceId = ? AND sequenceNr > ?";
        return toScala(storage.sql(sql, Long.class, persistenceId, fromSequenceNr)
                .thenApply(items -> {
                    long highestSequenceNr = items.findFirst().orElse(fromSequenceNr);
                    log.debug("doAsyncReadHighestSequenceNr '{}' {} -> {}", persistenceId, fromSequenceNr, highestSequenceNr);
                    return highestSequenceNr;
                }));
    }

    @Override
    public Future<Iterable<Optional<Exception>>> doAsyncWriteMessages(Iterable<AtomicWrite> messages) {
        return toScala(storage.insert(Streams.stream(messages)
                .map(atomicWrite -> stream(atomicWrite.payload())
                        .map(this::convert)
                        .collect(Collectors.toMap(JournalItem::id, Function.identity())))
                .collect(Collectors.toList())));
    }

    @Override
    public Future<Void> doAsyncDeleteMessagesTo(String persistenceId, long toSequenceNr) {
        log.debug("doAsyncDeleteMessagesTo '{}' {}", persistenceId, toSequenceNr);
        String sql = "DELETE FROM JournalItem WHERE persistenceId = ? AND sequenceNr <= ?";
        return toScala(storage.sql(sql, persistenceId, toSequenceNr));
    }

    private JournalItem convert(PersistentRepr p) {
        return new JournalItem(p.persistenceId(), p.sequenceNr(), serializer.toBinary(p));
    }

    private PersistentRepr convert(JournalItem item) {
        return (PersistentRepr) serializer.fromBinary(item.getPayload());
    }
}
