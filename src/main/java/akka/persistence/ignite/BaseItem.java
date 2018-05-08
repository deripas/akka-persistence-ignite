package akka.persistence.ignite;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@NoArgsConstructor
@AllArgsConstructor
@Data
public abstract class BaseItem implements Externalizable {

    private byte[] payload;

    public abstract Key id();

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(payload.length);
        out.write(payload);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        payload = new byte[in.readInt()];
        in.readFully(payload);
    }
}
