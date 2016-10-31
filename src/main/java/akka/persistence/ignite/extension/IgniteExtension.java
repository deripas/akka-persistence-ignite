package akka.persistence.ignite.extension;

import akka.actor.*;

/**
 * Created by anton on 21.10.16.
 */
public class IgniteExtension extends AbstractExtensionId<IgniteExtensionImpl> implements ExtensionIdProvider {

    public static final String IGNITE_CONFIG = "akka.persistence.ignite.config-file";
    public static final IgniteExtension EXTENSION = new IgniteExtension();

    private IgniteExtension() {
    }

    @Override
    public ExtensionId<? extends Extension> lookup() {
        return EXTENSION;
    }

    @Override
    public IgniteExtensionImpl createExtension(ExtendedActorSystem system) {
        return new IgniteExtensionImpl(system.settings().config().getString(IGNITE_CONFIG));
    }
}
