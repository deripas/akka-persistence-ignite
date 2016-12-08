package akka.persistence.ignite.executor;

import akka.actor.UntypedActor;

/**
 * Created by anton on 07.12.16.
 */
public class WorkerActor extends UntypedActor {

    @Override
    public void onReceive(Object message) throws Throwable {
        if (message instanceof WorkerActorTask) {
            sender().tell(((WorkerActorTask) message).call(), self());
        } else {
            unhandled(message);
        }
    }
}
