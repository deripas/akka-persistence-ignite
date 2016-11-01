package akka.persistence.ignite.snapshot;

import akka.dispatch.ExecutionContexts;
import akka.dispatch.Futures;
import akka.persistence.SelectedSnapshot;
import akka.persistence.SnapshotMetadata;
import akka.persistence.SnapshotSelectionCriteria;
import akka.persistence.ignite.extension.IgniteExtension;
import akka.persistence.serialization.Snapshot;
import akka.persistence.snapshot.japi.SnapshotStore;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.Future;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;

/**
 * Created by anton on 01.11.16.
 */
@Slf4j
public class IgniteSnapshotStore extends SnapshotStore {

    public static final String CACHE_PREFIX_PROPERTY = "cache-prefix";

    private final SnapshotItemCache snapshotItemCache;
    private final ExecutionContextExecutor contextExecutor;
    private final Serializer serializer;

    public IgniteSnapshotStore(Config config) {
        Ignite ignite = IgniteExtension.EXTENSION.get(context().system()).getIgnite();
        String cachePrefix = config.getString(CACHE_PREFIX_PROPERTY);
        snapshotItemCache = new SnapshotItemCache(ignite, cachePrefix);

        serializer = SerializationExtension.get(context().system()).serializerFor(Snapshot.class);

        //todo fix multi-treading
        contextExecutor = ExecutionContexts.fromExecutor(Executors.newSingleThreadExecutor());
    }

    @Override
    public Future<Optional<SelectedSnapshot>> doLoadAsync(String persistenceId, SnapshotSelectionCriteria criteria) {
        return Futures.future(() -> {
            log.debug("doLoadAsync '{}' {} {}", persistenceId, criteria.minSequenceNr(), criteria.maxSequenceNr());
            SnapshotItem snapshotItem = snapshotItemCache.findLast(persistenceId, criteria.minSequenceNr(), criteria.maxSequenceNr(), criteria.minTimestamp(), criteria.maxTimestamp());
            return Optional.ofNullable(convert(persistenceId, snapshotItem));
        }, contextExecutor);
    }

    @Override
    public Future<Void> doSaveAsync(SnapshotMetadata metadata, Object snapshot) {
        return Futures.future(() -> {
            log.debug("doSaveAsync '{}' ({})", metadata.persistenceId(), metadata.sequenceNr());
            snapshotItemCache.save(metadata.persistenceId(), convert(metadata, snapshot));
            return null;
        }, contextExecutor);
    }

    @Override
    public Future<Void> doDeleteAsync(SnapshotMetadata metadata) {
        return Futures.future(() -> {
            log.debug("doDeleteAsync '{}' ({})", metadata.persistenceId(), metadata.sequenceNr());
            snapshotItemCache.remove(metadata.persistenceId(), metadata.sequenceNr());
            return null;
        }, contextExecutor);
    }

    @Override
    public Future<Void> doDeleteAsync(String persistenceId, SnapshotSelectionCriteria criteria) {
        return Futures.future(() -> {
            log.debug("doDeleteAsync '{}' ({}; {})", persistenceId, criteria.minSequenceNr(), criteria.maxSequenceNr());
            Set<Long> keys = snapshotItemCache.remove(persistenceId, criteria.minSequenceNr(), criteria.maxSequenceNr(), criteria.minTimestamp(), criteria.maxTimestamp());
            log.debug("remove keys {}", keys);
            return null;
        }, contextExecutor);
    }


    private SnapshotItem convert(SnapshotMetadata metadata, Object snapshot) {
        return new SnapshotItem(metadata.sequenceNr(), metadata.timestamp(), serializer.toBinary(new Snapshot(snapshot)));
    }

    private SelectedSnapshot convert(String persistenceId, SnapshotItem item) {
        if (item == null) {
            return null;
        }
        SnapshotMetadata metadata = new SnapshotMetadata(persistenceId, item.getSequenceNr(), item.getTimestamp());
        Snapshot snapshot = (Snapshot) serializer.fromBinary(item.getPayload());
        return SelectedSnapshot.create(metadata, snapshot.data());
    }
}
