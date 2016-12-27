package akka.persistence.ignite.extension;

import akka.actor.*;
import akka.persistence.ignite.extension.impl.IgniteFactoryByConfigXml;
import lombok.*;
import org.apache.ignite.Ignite;
import scala.concurrent.ExecutionContextExecutor;

import java.util.function.Function;

/**
 * Created by anton on 21.10.16.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IgniteExtensionProvider extends AbstractExtensionId<IgniteExtension> implements ExtensionIdProvider {

    public static final IgniteExtensionProvider EXTENSION = new IgniteExtensionProvider();
    @Setter
    private Function<ExtendedActorSystem, Ignite> factory = new IgniteFactoryByConfigXml();

    @Override
    public ExtensionId<? extends Extension> lookup() {
        return EXTENSION;
    }

    @Override
    public IgniteExtension createExtension(ExtendedActorSystem system) {
        return new SimpleIgniteExtension(system.dispatcher(), factory.apply(system));
    }

    @AllArgsConstructor
    @Getter
    private class SimpleIgniteExtension implements IgniteExtension {
        private ExecutionContextExecutor dispatcher;
        private Ignite ignite;
    }
}
