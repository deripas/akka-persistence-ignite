package akka.persistence.ignite.journal;

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
public class JournalItem extends BaseItem implements Externalizable {

    private static final String JOURNAL_GROUP_IDX = "journal-group-idx";

    @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = JOURNAL_GROUP_IDX, order = 0)})
    private String persistenceId;

    @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = JOURNAL_GROUP_IDX, order = 1)})
    private long sequenceNr;

    public JournalItem(String persistenceId, long sequenceNr, byte[] payload) {
        super(payload);
        this.persistenceId = persistenceId;
        this.sequenceNr = sequenceNr;
    }

    @Override
    public Key id() {
        return new Key(persistenceId, sequenceNr);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(persistenceId);
        out.writeLong(sequenceNr);
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        persistenceId = in.readUTF();
        sequenceNr = in.readLong();
        super.readExternal(in);
    }
}
