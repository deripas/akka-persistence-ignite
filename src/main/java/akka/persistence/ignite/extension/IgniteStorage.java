package akka.persistence.ignite.extension;

import akka.actor.ActorSystem;
import akka.persistence.ignite.BaseItem;
import akka.persistence.ignite.Key;
import com.google.common.collect.Streams;
import com.typesafe.config.Config;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteFuture;
import scala.concurrent.ExecutionContextExecutor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by anton on 08.12.16.
 */
public class IgniteStorage<V extends BaseItem> {

    private final ExecutionContextExecutor dispatcher;
    private final IgniteCache<Key, V> cache;

    public IgniteStorage(Config config, ActorSystem actorSystem, String name) {
        IgniteExtension extension = IgniteExtensionProvider.EXTENSION.get(actorSystem);
        dispatcher = extension.getDispatcher();
        cache = extension.getIgnite().getOrCreateCache(name);
    }

    public CompletionStage<Void> insert(V item) {
        return toCS(cache.putAsync(item.id(), item));
    }

    //todo fix it, add multi-thread
    public CompletionStage<Iterable<Optional<Exception>>> insert(Iterable<Map<Key, V>> collect) {
        return CompletableFuture.supplyAsync(() -> Streams.stream(collect)
                .map(batch -> {
                    try {
                        cache.putAll(batch);
                        return Optional.<Exception>empty();
                    } catch (Exception e) {
                        return Optional.of(e);
                    }
                }).collect(Collectors.toList()), dispatcher);
    }

    public <S> CompletionStage<Stream<S>> sql(String sql, Class<S> result, Object... args) {
        return CompletableFuture.supplyAsync(() -> {
            List<List<?>> list = cache.query(sqlQuery(sql, args)).getAll();
            return listsToStream(list, result);
        }, dispatcher);
    }

    public CompletionStage<Void> sql(String sql, Object... args) {
        return CompletableFuture.runAsync(() -> {
            cache.query(sqlQuery(sql, args)).getAll();
        }, dispatcher);
    }

    private SqlFieldsQuery sqlQuery(String sql, Object... arg) {
        return new SqlFieldsQuery(sql).setArgs(arg);
    }

    private static <T> Stream<T> listsToStream(List<List<?>> list, Class<T> type) {
        return list.stream().flatMap(Collection::stream).filter(type::isInstance).map(type::cast);
    }

    private static <T> CompletionStage<T> toCS(IgniteFuture<T> future) {
        CompletableFuture<T> cf = new CompletableFuture<>();
        future.listen(igniteFuture -> {
            try {
                cf.complete(igniteFuture.get());
            } catch (Exception e) {
                cf.completeExceptionally(e);
            }
        });
        return cf;
    }
}
