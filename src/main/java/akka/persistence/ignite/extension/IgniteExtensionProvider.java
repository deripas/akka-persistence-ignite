package akka.persistence.ignite.extension;

import akka.actor.AbstractExtensionId;
import akka.actor.ExtendedActorSystem;
import akka.actor.Extension;
import akka.actor.ExtensionId;
import akka.actor.ExtensionIdProvider;
import akka.dispatch.MessageDispatcher;
import akka.persistence.ignite.extension.impl.IgniteFactoryByConfigXml;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.ignite.Ignite;
import scala.concurrent.ExecutionContextExecutor;

import java.util.function.Function;

/**
 * Created by anton on 21.10.16.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IgniteExtensionProvider extends AbstractExtensionId<IgniteExtension> implements ExtensionIdProvider {

    public static final String IGNITE_DISPATCHER = "ignite-dispatcher";
    public static final IgniteExtensionProvider EXTENSION = new IgniteExtensionProvider();

    @Setter
    private Function<ExtendedActorSystem, Ignite> factory = new IgniteFactoryByConfigXml();

    @Override
    public ExtensionId<? extends Extension> lookup() {
        return EXTENSION;
    }

    @Override
    public IgniteExtension createExtension(ExtendedActorSystem system) {
        MessageDispatcher dispatcher = system.dispatchers().lookup(IGNITE_DISPATCHER);
        return new SimpleIgniteExtension(dispatcher, factory.apply(system));
    }

    @AllArgsConstructor
    @Getter
    private class SimpleIgniteExtension implements IgniteExtension {
        private ExecutionContextExecutor dispatcher;
        private Ignite ignite;
    }
}
