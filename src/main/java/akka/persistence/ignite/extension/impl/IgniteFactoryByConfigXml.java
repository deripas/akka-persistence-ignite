package akka.persistence.ignite.extension.impl;

import akka.actor.ExtendedActorSystem;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

import java.util.function.Function;

/**
 * Created by anton on 21.10.16.
 */
@Slf4j
public class IgniteFactoryByConfigXml implements Function<ExtendedActorSystem, Ignite> {

    public static final String CONFIG = "ignite-config.file";

    @Override
    public Ignite apply(ExtendedActorSystem system) {
        Config config = system.settings().config();
        String configXml = config.getString(CONFIG);
        return Ignition.start(configXml);
    }
}
