package simpledb.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author nick
 * @e-mail cz739@nyu.edu
 * 2023/10/24
 */
public abstract class BaseKeyedResourceLockPool<V> implements KeyedResourceLockPool<V> {

    private final ConcurrentMap<V, ResourceLocker> locks = new ConcurrentHashMap<>();

    @Override
    public ResourceLocker getOrCreate(V key) {
        return locks.computeIfAbsent(key, k->new ResourceLocker());
    }

    @Override
    public void release(V key) {

    }
}
