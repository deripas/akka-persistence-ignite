package akka.persistence.ignite.snapshot;

import akka.persistence.ignite.BaseItem;
import akka.persistence.ignite.Key;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@Data
public class SnapshotItem extends BaseItem implements Externalizable {

    private static final String SNAPSHOT_GROUP_IDX = "snapshot-group-idx";

    @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = SNAPSHOT_GROUP_IDX, order = 0)})
    private String persistenceId;

    @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = SNAPSHOT_GROUP_IDX, order = 1)})
    private long sequenceNr;

    @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = SNAPSHOT_GROUP_IDX, order = 3)})
    private long timestamp;

    public SnapshotItem(String persistenceId, long sequenceNr, long timestamp, byte[] payload) {
        super(payload);
        this.persistenceId = persistenceId;
        this.sequenceNr = sequenceNr;
        this.timestamp = timestamp;
    }

    @Override
    public Key id() {
        return new Key(persistenceId, sequenceNr);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(persistenceId);
        out.writeLong(sequenceNr);
        out.writeLong(timestamp);
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        persistenceId = in.readUTF();
        sequenceNr = in.readLong();
        timestamp = in.readLong();
        super.readExternal(in);
    }
}
