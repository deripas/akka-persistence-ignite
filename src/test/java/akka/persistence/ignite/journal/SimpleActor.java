package akka.persistence.ignite.journal;

import akka.actor.ActorRef;
import akka.persistence.RecoveryCompleted;
import akka.persistence.UntypedPersistentActor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by anton on 08.12.16.
 */
@Slf4j
public class SimpleActor extends UntypedPersistentActor {

    private String id;
    private List<String> list;

    public SimpleActor(String id) {
        this.id = id;
        list = new ArrayList<>();
    }

    @Override
    public void onReceiveRecover(Object o) throws Throwable {
        log.debug("onReceiveRecover {}", o.toString());
        if (o instanceof String) {
            putCmd(o.toString());
        }
        if(o instanceof RecoveryCompleted) {
            log.debug("done");
        }
    }

    @Override
    public void onReceiveCommand(Object o) throws Throwable {
        log.debug("onReceiveCommand {}", o);
        if (o instanceof String) {
            String str = o.toString();
            if ("throw".equals(str)) {
                throw new RuntimeException();
            } else {
                persist(str, s -> {
                    putCmd(s);
                    if(sender() != ActorRef.noSender()) {
                        sender().tell(s, self());
                    }
                });
            }
        } else {
            unhandled(o);
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
