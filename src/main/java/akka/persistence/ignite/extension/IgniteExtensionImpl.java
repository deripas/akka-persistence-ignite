package akka.persistence.ignite.extension;

import akka.actor.Extension;
import com.typesafe.config.Config;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

/**
 * Created by anton on 21.10.16.
 */
@Slf4j
public class IgniteExtensionImpl implements Extension {

    @Getter
    private Ignite ignite;

    public IgniteExtensionImpl(String configXml) {
        ignite = Ignition.start(configXml);
    }
}
