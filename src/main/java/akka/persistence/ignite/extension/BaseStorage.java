package akka.persistence.ignite.extension;

import akka.actor.ActorSystem;
import akka.dispatch.Futures;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.typesafe.config.Config;
import lombok.Getter;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.Future;

import javax.annotation.Nonnull;
import java.util.function.Function;

/**
 * Created by anton on 08.12.16.
 */
public class BaseStorage<V> {
    private static final String CACHE_PREFIX_PROPERTY = "cache-prefix";
    private static final String DEFAULT_STORAGE = "default-storage";

    @Getter
    private final ExecutionContextExecutor dispatcher;
    private final LoadingCache<String, IgniteCache<Long, V>> cache;
    private final String cachePrefix;

    public BaseStorage(Config config, ActorSystem actorSystem, Class<V> valueClass) {
        IgniteExtension extension = IgniteExtensionProvider.EXTENSION.get(actorSystem);
        dispatcher = extension.getDispatcher();
        cachePrefix = config.getString(CACHE_PREFIX_PROPERTY);
        CacheConfiguration defaultConfig = findDefaultConfig(extension.getIgnite().configuration().getCacheConfiguration(), DEFAULT_STORAGE);
        cache = CacheBuilder.newBuilder().build(new CacheLoader<String, IgniteCache<Long, V>>() {
            @Override
            public IgniteCache<Long, V> load(@Nonnull String key) throws Exception {
                CacheConfiguration<Long, V> cfg = create(defaultConfig);
                cfg.setName(cachePrefix + key);
                cfg.setIndexedTypes(Long.class, valueClass);
                return extension.getIgnite().getOrCreateCache(cfg);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private CacheConfiguration<Long, V> create(CacheConfiguration defaultConfig) {
        return new CacheConfiguration<>(defaultConfig);
    }

    private CacheConfiguration findDefaultConfig(CacheConfiguration[] configurations, String name) {
        for (CacheConfiguration configuration : configurations) {
            if (configuration.getName().equals(name)) {
                return configuration;
            }
        }
        return new CacheConfiguration();
    }

    public <R> Future<R> execute(String key, Function<IgniteCache<Long, V>, R> function) {
        return Futures.future(() -> function.apply(cache.get(key)), dispatcher);
    }
}
