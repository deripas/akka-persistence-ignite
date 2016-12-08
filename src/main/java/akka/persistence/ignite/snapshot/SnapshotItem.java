package akka.persistence.ignite.snapshot;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;

/**
 * Created by anton on 01.11.16.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SnapshotItem implements Serializable {
    @QuerySqlField(index = true, descending = true)
    private long sequenceNr;
    @QuerySqlField(index = true, descending = true)
    private long timestamp;
    private byte[] payload;
}
