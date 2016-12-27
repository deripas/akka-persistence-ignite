package akka.persistence.ignite.extension;

import akka.actor.Extension;
import org.apache.ignite.Ignite;
import scala.concurrent.ExecutionContextExecutor;

/**
 * Created by anton on 27.12.16.
 */
public interface IgniteExtension extends Extension {

    ExecutionContextExecutor getDispatcher();

    Ignite getIgnite();
}
