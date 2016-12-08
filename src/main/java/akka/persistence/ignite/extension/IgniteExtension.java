package akka.persistence.ignite.extension;

import akka.actor.*;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Created by anton on 21.10.16.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IgniteExtension extends AbstractExtensionId<IgniteExtensionImpl> implements ExtensionIdProvider {

    public static final IgniteExtension EXTENSION = new IgniteExtension();

    @Override
    public ExtensionId<? extends Extension> lookup() {
        return EXTENSION;
    }

    @Override
    public IgniteExtensionImpl createExtension(ExtendedActorSystem system) {
        return new IgniteExtensionImpl(system);
    }
}
