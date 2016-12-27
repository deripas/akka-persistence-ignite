package akka.persistence.ignite.journal;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.persistence.ignite.extension.IgniteExtensionProvider;
import com.typesafe.config.ConfigFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Created by anton on 01.11.16.
 */
public class JournalItemCacheTest {

    private Ignite ignite;
    private ActorSystem actorSystem;

    @BeforeClass
    public void init() {
        actorSystem = ActorSystem.create("test", ConfigFactory.parseResources("test.conf"));
        ignite = IgniteExtensionProvider.EXTENSION.get(actorSystem).getIgnite();
    }

    @AfterClass
    public void destroy() {
        actorSystem.terminate();
    }

    @Test
    public void test1() throws Exception {
        ActorRef actorRef = actorSystem.actorOf(Props.create(SimpleActor.class, "1"));
        actorRef.tell("+a", ActorRef.noSender());
        actorRef.tell("+b", ActorRef.noSender());
        actorRef.tell("+c", ActorRef.noSender());
        Thread.sleep(1000);
        actorRef.tell("throw", ActorRef.noSender());

        Future<Object> future = Patterns.ask(actorRef, "-b", 1000);
        Await.result(future, Duration.create(1, TimeUnit.SECONDS));

        IgniteCache<Object, Object> cache = ignite.getOrCreateCache("akka-journal-1");
        Assert.assertEquals(cache.size(), 4);

        actorSystem.actorSelection("akka://test/user/**").tell("!!!", ActorRef.noSender());

        Await.result(actorSystem.terminate(), Duration.create(1, TimeUnit.SECONDS));;
    }

}