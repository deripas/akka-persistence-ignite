package akka.persistence.ignite.snapshot;

import akka.actor.ActorSystem;
import akka.persistence.SelectedSnapshot;
import akka.persistence.SnapshotMetadata;
import akka.persistence.SnapshotSelectionCriteria;
import akka.persistence.ignite.extension.IgniteStorage;
import akka.persistence.serialization.Snapshot;
import akka.persistence.snapshot.japi.SnapshotStore;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import com.google.common.annotations.Beta;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import scala.concurrent.Future;

import java.io.NotSerializableException;
import java.util.Optional;

import static scala.compat.java8.FutureConverters.toScala;

/**
 * Created by anton on 01.11.16.
 */
//todo not ready yet!
@Beta
@Slf4j
public class IgniteSnapshotStore extends SnapshotStore {

    public static final String IGNITE_SNAPSHOT_STORAGE = "ignite-snapshot-storage";

    private final Serializer serializer;
    private final IgniteStorage<SnapshotItem> storage;

    public IgniteSnapshotStore(Config config) throws NotSerializableException {
        ActorSystem system = context().system();
        serializer = SerializationExtension.get(system).serializerFor(Snapshot.class);
        storage = new IgniteStorage<>(config, system, IGNITE_SNAPSHOT_STORAGE);
    }

    @Override
    public Future<Optional<SelectedSnapshot>> doLoadAsync(String persistenceId, SnapshotSelectionCriteria criteria) {
        log.debug("doLoadAsync '{}' {}", persistenceId, criteria);
        String sql = "SELECT _val FROM SnapshotItem WHERE persistenceId = ? AND sequenceNr >= ? AND sequenceNr <= ? AND timestamp >= ? AND timestamp <= ?";
        return toScala(storage.sql(sql, SnapshotItem.class, persistenceId, criteria.minSequenceNr(), criteria.maxSequenceNr(), criteria.minTimestamp(), criteria.maxTimestamp())
                .thenApply(items -> items
                        .map(snapshotItem -> convert(persistenceId, snapshotItem))
                        .findFirst()));
    }

    @Override
    public Future<Void> doSaveAsync(SnapshotMetadata metadata, Object snapshot) {
        log.debug("doSaveAsync '{}' ({})", metadata.persistenceId(), metadata.sequenceNr());
        return toScala(storage.insert(convert(metadata, snapshot)));
    }

    @Override
    public Future<Void> doDeleteAsync(SnapshotMetadata metadata) {
        log.debug("doDeleteAsync {}", metadata);
        String sql = "DELETE FROM SnapshotItem WHERE persistenceId = ? AND sequenceNr <= ? AND timestamp <= ?";
        return toScala(storage.sql(sql, metadata.persistenceId(), metadata.sequenceNr(), metadata.timestamp()));
    }

    @Override
    public Future<Void> doDeleteAsync(String persistenceId, SnapshotSelectionCriteria criteria) {
        log.debug("doDeleteAsync '{}' {}", persistenceId, criteria);
        String sql = "DELETE FROM SnapshotItem WHERE persistenceId = ? AND sequenceNr >= ? AND sequenceNr <= ? AND timestamp >= ? AND timestamp <= ?";
        return toScala(storage.sql(sql, persistenceId, criteria.minSequenceNr(), criteria.maxSequenceNr(), criteria.minTimestamp(), criteria.maxTimestamp()));
    }

    private SnapshotItem convert(SnapshotMetadata meta, Object snapshot) {
        return new SnapshotItem(meta.persistenceId(), meta.sequenceNr(), meta.timestamp(), serializer.toBinary(new Snapshot(snapshot)));
    }

    private SelectedSnapshot convert(String persistenceId, SnapshotItem item) {
        SnapshotMetadata metadata = new SnapshotMetadata(persistenceId, item.getSequenceNr(), item.getTimestamp());
        Snapshot snapshot = (Snapshot) serializer.fromBinary(item.getPayload());
        return SelectedSnapshot.create(metadata, snapshot.data());
    }
}
