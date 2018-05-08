package akka.persistence.ignite;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by anton on 7.05.18.
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class Key implements Externalizable {

    @AffinityKeyMapped
    private String persistenceId;

    private long sequenceNr;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(persistenceId);
        out.writeLong(sequenceNr);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        persistenceId = in.readUTF();
        sequenceNr = in.readLong();
    }
}