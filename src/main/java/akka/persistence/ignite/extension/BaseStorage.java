package akka.persistence.ignite.extension;

import akka.pattern.Patterns;
import akka.persistence.ignite.executor.WorkerActorTask;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import scala.concurrent.Future;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * Created by anton on 08.12.16.
 */
public class BaseStorage<V> {
    private static final String CACHE_PREFIX_PROPERTY = "cache-prefix";
    private static final String EXEC_TIMEOUT_PROPERTY = "execute-timeout";

    private final LoadingCache<String, IgniteCache<Long, V>> cache;
    private final IgniteExtensionImpl extension;
    private final String cachePrefix;
    private final long timeout;

    public BaseStorage(Config config, IgniteExtensionImpl extension, Class<V> valueClass) {
        this.extension = extension;
        cachePrefix = config.getString(CACHE_PREFIX_PROPERTY);
        timeout = config.getLong(EXEC_TIMEOUT_PROPERTY);
        cache = CacheBuilder.newBuilder().build(new CacheLoader<String, IgniteCache<Long, V>>() {
            @Override
            public IgniteCache<Long, V> load(@Nonnull String key) throws Exception {
                CacheConfiguration<Long, V> cfg = new CacheConfiguration<>(cachePrefix + key);
                // todo need configure
                cfg.setCacheMode(CacheMode.PARTITIONED);
                cfg.setIndexedTypes(Long.class, valueClass);
                return extension.getIgnite().getOrCreateCache(cfg);
            }
        });
    }

    public <R> Future<R> execute(String key, Function<IgniteCache<Long, V>, R> function) {
        return (Future<R>) Patterns.ask(extension.getWorkerPool(), new WorkerActorTask<R>(key) {
            @Override
            public R call() throws Exception {
                return function.apply(cache.get(key));
            }
        }, timeout);
    }
}
