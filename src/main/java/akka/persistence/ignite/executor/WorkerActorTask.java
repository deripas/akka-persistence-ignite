package akka.persistence.ignite.executor;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.concurrent.Callable;

/**
 * Created by anton on 08.12.16.
 */
@Getter
@AllArgsConstructor
public abstract class WorkerActorTask<T> implements Callable<T> {
    private String persistenceId;
}
