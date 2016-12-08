package akka.persistence.ignite.snapshot;

import akka.actor.ActorSystem;
import akka.persistence.SelectedSnapshot;
import akka.persistence.SnapshotMetadata;
import akka.persistence.SnapshotSelectionCriteria;
import akka.persistence.ignite.extension.BaseStorage;
import akka.persistence.ignite.extension.IgniteExtension;
import akka.persistence.serialization.Snapshot;
import akka.persistence.snapshot.japi.SnapshotStore;
import akka.serialization.SerializationExtension;
import akka.serialization.Serializer;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import scala.concurrent.Future;

import javax.cache.Cache;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by anton on 01.11.16.
 */
@Slf4j
public class IgniteSnapshotStore extends SnapshotStore {

    private final Serializer serializer;
    private final BaseStorage<SnapshotItem> storage;

    public IgniteSnapshotStore(Config config) {
        ActorSystem actorSystem = context().system();
        serializer = SerializationExtension.get(actorSystem).serializerFor(Snapshot.class);
        storage = new BaseStorage<>(config, IgniteExtension.EXTENSION.get(actorSystem), SnapshotItem.class);
    }

    @Override
    public Future<Optional<SelectedSnapshot>> doLoadAsync(String persistenceId, SnapshotSelectionCriteria criteria) {
        return storage.execute(persistenceId, cache -> {
            log.debug("doLoadAsync '{}' {} {}", persistenceId, criteria.minSequenceNr(), criteria.maxSequenceNr());
            try (QueryCursor<Cache.Entry<Long, SnapshotItem>> query = cache
                    .query(new SqlQuery<Long, SnapshotItem>(SnapshotItem.class, "sequenceNr >= ? AND sequenceNr <= ? AND timestamp >= ? AND timestamp <= ?")
                            .setArgs(criteria.minSequenceNr(), criteria.maxSequenceNr(), criteria.minTimestamp(), criteria.maxTimestamp()))) {
                Iterator<Cache.Entry<Long, SnapshotItem>> iterator = query.iterator();
                return Optional.ofNullable(iterator.hasNext() ? convert(persistenceId, iterator.next().getValue()) : null);
            }
        });
    }

    @Override
    public Future<Void> doSaveAsync(SnapshotMetadata metadata, Object snapshot) {
        return storage.execute(metadata.persistenceId(), cache -> {
            log.debug("doSaveAsync '{}' ({})", metadata.persistenceId(), metadata.sequenceNr());
            SnapshotItem item = convert(metadata, snapshot);
            cache.put(item.getSequenceNr(), item);
            return null;
        });
    }

    @Override
    public Future<Void> doDeleteAsync(SnapshotMetadata metadata) {
        return storage.execute(metadata.persistenceId(), cache -> {
            log.debug("doDeleteAsync '{}' ({})", metadata.persistenceId(), metadata.sequenceNr());
            cache.remove(metadata.sequenceNr());
            return null;
        });
    }

    @Override
    public Future<Void> doDeleteAsync(String persistenceId, SnapshotSelectionCriteria criteria) {
        return storage.execute(persistenceId, cache -> {
            log.debug("doDeleteAsync '{}' ({}; {})", persistenceId, criteria.minSequenceNr(), criteria.maxSequenceNr());
            List<List<?>> seq = cache
                    .query(new SqlFieldsQuery("select sequenceNr from SnapshotItem where sequenceNr >= ? AND sequenceNr <= ? AND timestamp >= ? AND timestamp <= ?")
                            .setArgs(criteria.minSequenceNr(), criteria.maxSequenceNr(), criteria.minTimestamp(), criteria.maxTimestamp()))
                    .getAll();
            Set<Long> keys = listsToSetLong(seq);
            log.debug("remove keys {}", keys);
            cache.removeAll(keys);
            return null;
        });
    }

    private SnapshotItem convert(SnapshotMetadata metadata, Object snapshot) {
        return new SnapshotItem(metadata.sequenceNr(), metadata.timestamp(), serializer.toBinary(new Snapshot(snapshot)));
    }

    private SelectedSnapshot convert(String persistenceId, SnapshotItem item) {
        SnapshotMetadata metadata = new SnapshotMetadata(persistenceId, item.getSequenceNr(), item.getTimestamp());
        Snapshot snapshot = (Snapshot) serializer.fromBinary(item.getPayload());
        return SelectedSnapshot.create(metadata, snapshot.data());
    }

    private static Set<Long> listsToSetLong(List<List<?>> list) {
        return list.stream().flatMap(Collection::stream).filter(o -> o instanceof Long).map(o -> (Long) o).collect(Collectors.toSet());
    }
}
