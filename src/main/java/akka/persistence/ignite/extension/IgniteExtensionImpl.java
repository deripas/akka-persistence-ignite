package akka.persistence.ignite.extension;

import akka.actor.ActorRef;
import akka.actor.ExtendedActorSystem;
import akka.actor.Extension;
import akka.actor.Props;
import akka.dispatch.MessageDispatcher;
import akka.persistence.ignite.executor.WorkerActor;
import akka.persistence.ignite.executor.WorkerActorTask;
import akka.routing.ConsistentHashingPool;
import com.typesafe.config.Config;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

/**
 * Created by anton on 21.10.16.
 */
@Slf4j
@Getter
public class IgniteExtensionImpl implements Extension {

    public static final String CONFIG = "akka.persistence.ignite.config-file";
    public static final String DISPATCHER = "akka.persistence.ignite.dispatcher";
    public static final String NR_OF_INSTANCES = "akka.persistence.ignite.nrOfInstances";

    private final Ignite ignite;
    private final ActorRef workerPool;
    private final MessageDispatcher messageDispatcher;

    public IgniteExtensionImpl(ExtendedActorSystem system) {
        Config config = system.settings().config();
        String configXml = config.getString(CONFIG);
        String dispatcher = config.getString(DISPATCHER);
        ignite = Ignition.start(configXml);
        workerPool = system.actorOf(new ConsistentHashingPool(config.getInt(NR_OF_INSTANCES))
                .withHashMapper(message -> message instanceof WorkerActorTask ? ((WorkerActorTask) message).getPersistenceId() : null)
                .withDispatcher(dispatcher)
                .props(Props.create(WorkerActor.class)), "journal-pool");
        messageDispatcher = system.dispatchers().lookup(dispatcher);
    }
}
