package akka.persistence.ignite.journal;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.persistence.AbstractPersistentActor;
import akka.persistence.DeleteMessagesFailure;
import akka.persistence.DeleteMessagesSuccess;
import akka.persistence.RecoveryCompleted;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Created by anton on 08.12.16.
 */
@Slf4j
public class SimpleActor extends AbstractPersistentActor {

    private final String id;
    private final List<String> list;
    private final CompletableFuture clearFuture = new CompletableFuture();

    public SimpleActor(String id) {
        this.id = id;
        list = new ArrayList<>();
    }

    public static Props props(String id) {
        return Props.create(SimpleActor.class, id);
    }

    @Override
    public Receive createReceiveRecover() {
        return ReceiveBuilder.create()
                .match(String.class, str -> {
                    putCmd(str);
                    log.debug("onReceiveRecover {}", str);
                })
                .match(RecoveryCompleted.class, c -> {
                    log.debug("done");
                })
                .build();
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .matchEquals("throw", s -> {
                    throw new RuntimeException();
                })
                .matchEquals("clear", s -> {
                    deleteMessages(lastSequenceNr());
                    list.clear();
                    ActorRef sender = sender();
                    clearFuture.thenAccept(o -> sender.tell("clear ready", ActorRef.noSender()));
                })
                .match(String.class, str -> {
                    persist(str, s -> {
                        putCmd(s);
                        reply(s);
                    });
                })
                .match(DeleteMessagesSuccess.class, deleteMessagesSuccess -> {
                    clearFuture.complete(deleteMessagesSuccess);
                })
                .match(DeleteMessagesFailure.class, deleteMessagesFailure -> {
                    deleteMessagesFailure.cause().printStackTrace();
                    clearFuture.completeExceptionally(deleteMessagesFailure.cause());
                })
                .build();
    }

    private void reply(String str) {
        if (sender() != ActorRef.noSender()) {
            sender().tell(str, self());
        }
    }

    private void putCmd(String s) {
        if (s.startsWith("+")) {
            list.add(s.substring(1));
        }
        if (s.startsWith("-")) {
            list.remove(s.substring(1));
        }
    }

    @Override
    public String persistenceId() {
        return id;
    }
}
